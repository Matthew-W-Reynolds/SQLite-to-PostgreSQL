# *************************************************************************************************
# Name:
#   migrate.py v1.0.0
# Author:
#   Matthew Reynolds, 02/06/2022.
# Description:
#   Python script to convert the contents of the sakila-dump.sql file from SQLite syntax to
#   PostgreSQL compatible syntax.
#
#   You must have a <database-name>-dump.sql file from a SQLite database.
#   I used DB Browser for SQLite. Place it into the same directory as the migrate.py file.
#
# *************************************************************************************************
#
#   NOTE: Run this sql statement on the new PostgreSQL database to generate a series statements
#         to update the sequences. Source: https://wiki.postgresql.org/wiki/Fixing_Sequences
#
#         SELECT 'SELECT SETVAL(' ||
#             quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
#             ', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
#             quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';'
#         FROM pg_class AS S,
#             pg_depend AS D,
#             pg_class AS T,
#             pg_attribute AS C,
#             pg_tables AS PGT
#         WHERE S.relkind = 'S'
#             AND S.oid = D.objid
#             AND D.refobjid = T.oid
#             AND D.refobjid = C.attrelid
#             AND D.refobjsubid = C.attnum
#             AND T.relname = PGT.tablename
#         ORDER BY S.relname;
#
# Which produces This:
#
# SELECT SETVAL('public.actor_actor_id_seq', COALESCE(MAX(actor_id), 1) ) FROM public.actor;
# SELECT SETVAL('public.address_address_id_seq', COALESCE(MAX(address_id), 1) ) FROM public.address;
# SELECT SETVAL('public.category_category_id_seq', COALESCE(MAX(category_id), 1) ) FROM public.category;
# SELECT SETVAL('public.city_city_id_seq', COALESCE(MAX(city_id), 1) ) FROM public.city;
# SELECT SETVAL('public.country_country_id_seq', COALESCE(MAX(country_id), 1) ) FROM public.country;
# SELECT SETVAL('public.customer_customer_id_seq', COALESCE(MAX(customer_id), 1) ) FROM public.customer;
# SELECT SETVAL('public.film_film_id_seq', COALESCE(MAX(film_id), 1) ) FROM public.film;
# SELECT SETVAL('public.inventory_inventory_id_seq', COALESCE(MAX(inventory_id), 1) ) FROM public.inventory;
# SELECT SETVAL('public.language_language_id_seq', COALESCE(MAX(language_id), 1) ) FROM public.language;
# SELECT SETVAL('public.payment_payment_id_seq', COALESCE(MAX(payment_id), 1) ) FROM public.payment;
# SELECT SETVAL('public.rental_rental_id_seq', COALESCE(MAX(rental_id), 1) ) FROM public.rental;
# SELECT SETVAL('public.staff_staff_id_seq', COALESCE(MAX(staff_id), 1) ) FROM public.staff;
# SELECT SETVAL('public.store_store_id_seq', COALESCE(MAX(store_id), 1) ) FROM public.store;
#
#
CONST_INPUT_FILE_NAME = "sakila-dump.sql"
CONST_OUTPUT_FILE_NAME = "sakila-converted-to-postgres.sql"

CONST_SERIAL_PRIMARY_KEY ="SERIAL PRIMARY KEY,\n"   # This is the PostgreSQL syntax for an AUTOINCREMENT PRIMARY KEY.
CONST_PRIMARY_KEY = "PRIMARY KEY("                  # Need to get the primary key field name if it's an AUTOINCREMENT so we can modify the syntax.
CONST_FOREIGN_KEY = "FOREIGN KEY"                   # All foreign keys are buffered to fk_buffer until all CREATE TABLE declarations are done and all INSERTs are done.
                                                    # Then a sequence of ALTER TABLE <table name> ADD CONSTRAINT ..... are added.
CONST_CREATE_TABLE = "CREATE TABLE"                 # CREATE TABLE statement, used to buffer all successive lines until we hit ); and we can start modifying syntax.
CONST_CREATE_TRIGGER_START = "CREATE TRIGGER"       # A Trigger declaration has been found. It's ignored as it's not required by the AWS re/Start Assginment 20022.
CONST_CREATE_TRIGGER_END = "END;\n"                 #
CONST_CREATE_VIEW_START = "CREATE VIEW"             # A View declaration has been found. It's ignored as it's not required by the AWS re/Start Assginment 20022.
CONST_CREATE_VIEW_END = ";\n"                       #
CONST_BOOL_TRUE = "TRUE"                            # Boolean value changes to 1 (INTEGER).
CONST_BOOL_FALSE = "FALSE"                          # Boolean value changes to 0 (INTEGER).
CONST_EXISTS = "EXISTS "                            # This is used to grab the table name.
CONST_AUTOINCREMENT = " AUTOINCREMENT"              # This keyword is REMOVED. The PK datatype is changed to SERIAL.
CONST_UNSIGNED = "UNSIGNED"                         # This keyword is REMOVED.
CONST_DEFAULT = " DEFAUL"                           # film.special_features had NO datatype. This helps to find it in the script and swap in TEXT.
CONST_DATATYPE_ENUM = "ENUM"                        # Replace ENUM (SQLite)
CONST_DATATYPE_YEAR = "YEAR"                        # Replace YEAR (SQLite)
CONST_DATATYPE_TEXT = "TEXT"                        # with TEXT (PostgreSQL)
CONST_DATATYPE_DATEIME = "DATETIME"                 # Replace DATETIME (SQLite)
CONST_DATATYPE_TIMESTAMP = "TIMESTAMP"              # with TIMESTAMP (PostgreSQL)
CONST_DATATYPE_BOOLEAN = "BOOLEAN"                  # Replace BOOLEAN (SQLite)
CONST_DATATYPE_INT_DEFAULT = "INT DEFAULT"          # Replace INT (SQLite)
CONST_DATATYPE_MEDIUMINT = "MEDIUMINT"              # Replace MEDIUMINT (SQLite)
CONST_DATATYPE_INTEGER = "INTEGER"                  # with INTEGER (PostgreSQL)
CONST_DATATYPE_TINYINT = "TINYINT"                  # Replace TINYINT (SQLite)
CONST_DATATYPE_SMALLINT = "SMALLINT"                # with SMALLINT (PostgreSQL)
CONST_DATATYPE_BLOB = "BLOB"                        # Replace BLOB (SQLite)
CONST_DATATYPE_BYTEA = "BYTEA"                      # with BYTEA (PostgreSQL)

tableCount = 0                                      # NOTE: Was used during development and de-bugging. Let it in as it gives a sense of progress.
currentTableName = ""                               # When a CREATE TABLE statement is encountered, keep copy of table name in this var.
foundTable = False
foundView = False
foundTrigger = False
foundTransaction = False
buffer = []                                         # Used to hold the contents of a CREATE TABLE statement so the syntax can be modified.
fk_buffer = []                                      # Used to hold ALL foreign key constraints until after ALL INSERT statements have been written out.

def get_table_name(a_line: str):
    t_name = ""
    pt1 = a_line.find(CONST_EXISTS)
    if pt1 >= 0:
        pt1 += len(CONST_EXISTS)
        pt2 = a_line.find(" (")
        if pt2 > pt1:
            t_name = a_line[pt1:pt2]
    return t_name

def modify_primary_key_syntax(field_name: str):
    pt1 = field_name.find(" ")
    if pt1 >= 0:
        pt1 += 1
        return field_name[:pt1] + CONST_SERIAL_PRIMARY_KEY
    else:
        return field_name

# open the dump file to read in
#
with open(CONST_INPUT_FILE_NAME, "r") as sqlite_f:
    # open a file to write out the modified lines of text
    #
    with open(CONST_OUTPUT_FILE_NAME, "w") as postgres_f:
        # Loop through the sakila-dump.sql file line-by-line and modify the syntax if needed
        #
        for line in sqlite_f:
            # These are easy straight swaps so lets perform a series of Find & Replace statements
            #
            line = line.replace("\r", "")
            line = line.replace("\t", " ").strip() + "\n"
            line = line.replace('"', "")
            line = line.replace(CONST_DATATYPE_DATEIME, CONST_DATATYPE_TIMESTAMP)
            line = line.replace(CONST_UNSIGNED, "")
            if line.find(CONST_DATATYPE_BOOLEAN) >= 0:
                line = line.replace(CONST_DATATYPE_BOOLEAN, CONST_DATATYPE_INTEGER)
                if line.find(CONST_BOOL_TRUE) >= 0:
                    line = line.replace(CONST_BOOL_TRUE, "1")
                elif line.find(CONST_BOOL_FALSE) >= 0:
                    line = line.replace(CONST_BOOL_FALSE, "0")
            line = line.replace(CONST_DATATYPE_ENUM, CONST_DATATYPE_TEXT)
            line = line.replace(CONST_DATATYPE_YEAR, CONST_DATATYPE_TEXT)
            line = line.replace(CONST_DATATYPE_INT_DEFAULT, CONST_DATATYPE_INTEGER + " DEFAULT")
            line = line.replace(CONST_DATATYPE_MEDIUMINT, CONST_DATATYPE_INTEGER)
            line = line.replace(CONST_DATATYPE_TINYINT, CONST_DATATYPE_SMALLINT)
            line = line.replace(CONST_DATATYPE_BLOB, CONST_DATATYPE_BYTEA)

            # Looking for a field name with NO datatype and just setting it to TEXT.
            # It only occurs once in the sakila.dump file
            #
            pt = line.find(" ")
            if pt > 0:
                if line[pt+1:pt+8] == CONST_DEFAULT: line = line[:pt+1] + CONST_DATATYPE_TEXT + line[pt+1:]

            # If a CREATE TABLE statement has been found then
            # reset the buffer and set the boolean foundTable to true
            #
            if line.startswith(CONST_CREATE_TABLE):
                #print("Found a CREATE TABLE line")
                buffer = []
                currentTableName = get_table_name(line)
                foundTable = True
            
            # If we have a FOREIGN KEY store it into the fk_buffer list as a dictionary for later processing
            # and then throw the line away so that it's not processed further.
            #
            if line.find(CONST_FOREIGN_KEY) >= 0:
                if currentTableName != "":
                    fk_buffer.append({ "table_name" : currentTableName, "fk" : line })
                    line = ""
            
            # If a CREATE TRIGGER statement has been found then
            # ignore the rest of the text until END;
            #
            if line.startswith(CONST_CREATE_TRIGGER_START): foundTrigger = True
            
            # If a CREATE VIEW statement has been found then
            # ignore the rest of the text until ;
            #
            if line.startswith(CONST_CREATE_VIEW_START): foundView = True

            # If we've found a CREATE TABLE statement then keep reading in the lines
            # and add each one to the buffer[] list. We will modify the syntax once we
            # have hit the ); characters which when foundTable == True means we've
            # reached the end of the CREATE TABLE statement.
            #
            if foundTable == True:
                buffer.append(line)

                # Look for the end of the CREATE TABLE statement
                #
                if line.startswith(");"):
                    pk_field_name = ""
                    foundTable = False

                    # Now we look for PIMARY KEYS and change the syntax
                    #
                    for idx in range(len(buffer)):
                        a_line = buffer[idx]

                        # If we find the keywords PRIMARY KEY in the line
                        #
                        pt1 = a_line.find(CONST_PRIMARY_KEY)
                        if pt1 >= 0:
                            pt1 = pt1 + len(CONST_PRIMARY_KEY)

                            pt2 = a_line.find(" ", pt1)
                            if pt2 >= pt1:
                                # Now we look for CONST_AUTOINCREMENT
                                # 
                                if a_line.find(CONST_AUTOINCREMENT) >= 0:
                                    # We extract out the field name for the PRIMARY KEY if it's an AUTOINCREMENT
                                    # so we can change the syntax
                                    #
                                    pk_field_name = a_line[pt1:pt2]

                                    # We set this element in the list to empty string, so when we write out the buffer
                                    # we skip the empty string lines
                                    #
                                    buffer[idx] = ""
                                    break

                    # If we found a primary key field name thats an AUTOINCREMENT, so We start by looping through the buffer
                    # and when we find the first line that has the field name we modify this line's syntax
                    #
                    if pk_field_name != "":
                        for idx in range(len(buffer)):
                            a_line = buffer[idx]
                            pt1 = a_line.find(pk_field_name)
                            if pt1 >= 0:
                                buffer[idx] = modify_primary_key_syntax(a_line)
                                break
                    
                    # Let's check that the line of text before the ); *doesn't* have a comma , at the end of it
                    # If it does we remove the comma as it will cause an error in postgres
                    #
                    idx = len(buffer) - 1
                    for a_line in reversed(buffer):
                        if a_line != "" and a_line.startswith(");") == False:
                            if a_line.endswith(",\n"): buffer[idx] = a_line[:-2] + "\n"
                            break
                        idx -= 1
                    
                    # Now we write the modified buffer out to the file sqlite-converted-to-postgres.sql file
                    #
                    for a_line in buffer:
                        if (a_line != ""): postgres_f.write(a_line)

                    currentTableName = ""
                    tableCount += 1
                    print("tableCount=" + str(tableCount))

                    # NOTE: DEBUGGING CODE
                    #
                    # if tableCount == 16: break
                    # END DEBUGGING
            elif foundTrigger == True:
                # We ignore trigger delcarations completely
                #
                if (a_line.find(CONST_CREATE_TRIGGER_END) >= 0): foundTrigger = False
            elif foundView == True:
                # We ignore view delcarations completely
                #
                if (a_line.find(CONST_CREATE_VIEW_END) >= 0): foundView = False
            else:
                if (line != ""): postgres_f.write(line)
        
        if len(fk_buffer) > 0:
            for fk in fk_buffer:
                a_line = fk["fk"]
                if a_line.startswith("\t"): a_line = a_line[1:]
                if a_line.endswith(",\n"): a_line = a_line[:-2] + ";\n"
                postgres_f.write("ALTER TABLE " + fk["table_name"] + " ADD " + a_line)
        
        postgres_f.write("COMMIT;\n")
