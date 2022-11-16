from Connect import Connect

passwd = input("Por favor, ingrese la contraseña del administrador de la base de datos: ")
dbName = input("Por favor, ingrese el nombre de la base de datos a operar: ")
connect = Connect(dbName, 'postgres', passwd)
connect.execute("CREATE USER pad WITH ENCRYPTED PASSWORD 'padPasswd'")
connect.execute("GRANT ALL PRIVILEGES ON DATABASE "
                + dbName + " TO pad")
