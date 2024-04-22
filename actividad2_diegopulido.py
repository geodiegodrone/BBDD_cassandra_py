#Se realiza la importación de la herramienta "Cluster" de la librería, previamente instalada, "cassandra".
from cassandra.cluster import Cluster
#Se crean las variables "cluster" y "session" para poder utilizar la herramienta importada y realizar la conexión con la base de datos.
cluster = Cluster()
session = cluster.connect('diegopulido')

#Se procede a definir las entidades y relaciones del mapa conceptual.
class Cuenta:
    def __init__ (self, Cuenta_Numero, Cuenta_Saldo, Cuenta_Servicios): #Constructor sin relación 1:n entre Cuenta y Sucursal
        self.Cuenta_Numero = Cuenta_Numero
        self.Cuenta_Saldo = Cuenta_Saldo
        self.Cuenta_Servicios = Cuenta_Servicios
    def __init__ (self, Cuenta_Numero, Sucursal_id, Cuenta_Saldo, Cuenta_Servicios): #Constructor con relación 1:n entre Cuenta y Sucursal
        self.Cuenta_Numero = Cuenta_Numero
        self.Sucursal_id = Sucursal_id
        self.Cuenta_Saldo = Cuenta_Saldo
        self.Cuenta_Servicios = Cuenta_Servicios
class CuBen:
    def __init__ (self, Cuenta_Numero, Beneficiario_DNIPasaporte ):
        self.Cuenta_Numero = Cuenta_Numero
        self.Beneficiario_DNIPasaporte = Beneficiario_DNIPasaporte
class Depositante: 
    def __init__ (self, Cuenta_Numero, Cliente_DNIPasaporte):
        self.Cuenta_Numero = Cuenta_Numero
        self.Cliente_DNIPasaporte = Cliente_DNIPasaporte
class DetalleTar:
    def __init__(self,Tarjeta_Nombre, Cuenta_Numero, DetalleTar_Limite):
        self.Tarjeta_Nombre = Tarjeta_Nombre
        self.Cuenta_Numero = Cuenta_Numero
        self.DetalleTar_Limite = DetalleTar_Limite
class Beneficiario:
    def __init__ (self, Beneficiario_DNIPasaporte, Beneficiario_Nombre):
        self.Beneficiario_DNIPasaporte = Beneficiario_DNIPasaporte
        self.Beneficiario_Nombre = Beneficiario_Nombre
class Tarjeta:
    def __init__ (self, Tarjeta_Tipo, Tarjeta_Nombre):
        self.Tarjeta_Tipo = Tarjeta_Tipo
        self.Tarjeta_Nombre = Tarjeta_Nombre
class Sucursal:
    def __init__ (self, Sucursal_Nombre, Sucursal_Ciudad, Sucursal_Activo, Sucursal_id):
        self.Sucursal_Nombre = Sucursal_Nombre
        self.Sucursal_Ciudad = Sucursal_Ciudad
        self.Sucursal_Activo = Sucursal_Activo
        self.Sucursal_Id =Sucursal_id
class Prestamo:
    def __init__ (self, Prestamo_Numero, Prestamo_Cantidad): #Constructor sin relación 1:n entre Prestamo y Sucursal
        self.Prestamo_Numero = Prestamo_Numero
        self.Prestamo_Cantidad = Prestamo_Cantidad
    def __init__ (self, Prestamo_Numero, Prestamo_Cantidad, Sucursal_id): #Constructor con relación 1:n entre Prestamo y Sucursal
        self.Prestamo_Numero = Prestamo_Numero
        self.Prestamo_Cantidad = Prestamo_Cantidad
        self.Sucursal_id =Sucursal_id
class Cliente: 
    def __init__ (self, Cliente_DNIPasaporte,  Cliente_Nombre, Cliente_Calle, Cliente_Ciudad):
        self.Cliente_DNIPasaporte = Cliente_DNIPasaporte
        self.Cliente_Nombre = Cliente_Nombre
        self.Cliente_Calle = Cliente_Calle
        self.Cliente_Ciudad = Cliente_Ciudad
class Prestatario:
    def __init__ (self, Cliente_DNIPasaporte, Prestamo_Numero):
        self.Cliente_DNIPasaporte = Cliente_DNIPasaporte
        self.Prestamo_Numero = Prestamo_Numero
        
#Función para insertar instancias de Sucursal en tabla de consulta 1 y soporte:
def insertSucursal ():
    #Pedimos al usuario del programa los datos sobre la sucursal
    SucursalNombre = input('Escribe el nombre de la sucursal')
    SucursalCiudad = input('Escribe el nombre de la ciudad de la sucursal')
    SucursalActivo = input('Si está activo escribe cualquier letra, si no está activo oprime ENTER')
    Sucursalid = input('Escribe el id de la sucursal')
    
    s = Sucursal (SucursalCiudad, Sucursalid, SucursalNombre, SucursalActivo)
    insertStatementTabla1 = session.prepare ('INSERT INTO Tabla1 (Sucursal_Ciudad, Sucursal_id, Sucursal_Nombre, Sucursal_Activo) VALUES (?, ?, ?, ?)')
    session.execute (insertStatementTabla1, [s.Sucursal_Ciudad, s.Sucursal_id, s.Sucursal_Nombre, s.Sucursal_Activo])
      
    insertStatementSoporteSucursal = session.prepare ('INSERT INTO SoporteSucursal (Sucursalid, SucursalActivo, SucursalCiudad, SucursalNombre) VALUES (?, ?, ?, ?)')
    session.execute (insertStatementSoporteSucursal, [s.Sucursal_id, s.Sucursal_Activo, s.Sucursal_Ciudad, s.Sucursal_Nombre])

#Función para insertar instancias de Cuenta en tabla de consulta 7 y soporte:
def insertCuenta ():
    #Pedimos al usuario del programa los datos sobre la cuenta
    CuentaNumero = int(input('Escribe el número de cuenta'))
    CuentaSaldo = int(input('Escribe el saldo de la cuenta'))
    CuentaServicios = set() #Contendrá los servicios a insertar
    Servicio = input('Escribe un servicio o deja vacio para parar')
    while (Servicio != ''):
        CuentaServicios.add(Servicio)
        Servicio = input('Escribe un servicio o deja vacio para parar')
    
    c = Cuenta (CuentaNumero, CuentaServicios, CuentaSaldo)
    insertStatementTabla7 = session.prepare ('INSERT INTO Tabla7 (Cuenta_Numero, Cuenta_Servicios, Cuenta_Saldo) VALUES (?, ?, ?)')
    session.execute (insertStatementTabla7, [c.Cuenta_Numero, c.Cuenta_Servicios, c.Cuenta_Saldo])
         
    insertStatementSoporteCuenta = session.prepare ('INSERT INTO SoporteCuenta (SoporteCuenta_Numero, SoporteCuenta_Saldo, SoporteCuenta_Servicios) VALUES (?, ?, ?)')
    session.execute (insertStatementSoporteCuenta, [c.Cuenta_Numero, c.Cuenta_Saldo, c.Cuenta_Servicios])
    
#Función para insertar instancias de Cliente en la tabla de consulta 6 y soporte:
def insertCliente ():
    #Pedimos al usuario del programa los datos sobre el cliente
    ClienteDNIPasaporte = input('Escribe la identificación del cliente')
    ClienteNombre = input('Escribe el nombre del cliente')
    ClienteCalle = input('Escribe la dirección del cliente') 
    ClienteCiudad = input('Escribe la ciudad del cliente')
    
    cl = Cliente (ClienteCiudad, ClienteDNIPasaporte, ClienteNombre, ClienteCalle)
    insertStatementTabla6 = session.prepare ('INSERT INTO Tabla6 (Cliente_Ciudad, Cliente_DNIPasaporte, Cliente_Nombre, Cliente_Calle) VALUES (?, ?, ?, ?)')
    session.execute (insertStatementTabla6, [cl.Cliente_Ciudad, cl.Cliente_DNIPasaporte, cl.Cliente_Nombre, cl.Cliente_Calle])
    
    insertStatementSoporteCliente = session.prepare ('INSERT INTO SoporteCliente (SoporteCliente_DNIPasaporte, SoporteCliente_Nombre, SoporteCliente_Calle, SoporteCliente_Ciudad) VALUES (?, ?, ?, ?)')
    session.execute (insertStatementSoporteCliente, [cl.Cliente_DNIPasaporte, cl.Cliente_Nombre, cl.Cliente_Calle, cl.Cliente_Ciudad])
    
#Función para insertar instancias de la relación Depositante (cuenta-cliente):
def insertDepositante ():
    ClienteDNIPasaporte = input('Escribe la identificación del cliente')
    ClienteNombre = input('Escribe el nombre del cliente')
    ClienteCalle = input('Escribe la dirección del cliente') 
    ClienteCiudad = input('Escribe la ciudad del cliente')
    CuentaNumero = int(input('Escribe el número de cuenta'))
    CuentaSaldo = int(input('Escribe el saldo de la cuenta'))
    CuentaServicios = set() #Contendrá los servicios a insertar
    Servicio = input('Escribe un servicio o deja vacio para parar')
    while (Servicio != ''):
        CuentaServicios.add(Servicio)
        Servicio = input('Escribe un servicio o deja vacio para parar')
    
    insertStatementTabla6 = session.prepare ('INSERT INTO Tabla6 (Cliente_Ciudad, Cliente_DNIPasaporte, Cliente_Nombre, Cliente_Calle) VALUES (?, ?, ?, ?)')
    session.execute (insertStatementTabla6, [ClienteCiudad, ClienteDNIPasaporte, ClienteNombre, ClienteCalle])
    
    insertStatementTabla7 = session.prepare ('INSERT INTO Tabla7 (Cuenta_Numero, Cuenta_Servicios, Cuenta_Saldo) VALUES (?, ?, ?)')
    session.execute (insertStatementTabla7, [CuentaNumero, CuentaServicios, CuentaSaldo])
    
#Función para insertar instancias de la relación DetalleTar (tarjeta-cuenta):
def insertDetalleTar ():
    TarjetaNombre = input('Escbribe el nombre de la tarjeta')
    TarjetaTipo = input('Escribe el tipo de tarjeta')
    TarjetaLimite = float(input('Escribe el valor límite de la tarjeta'))
    CuentaNumero = int(input('Escribe el número de cuenta'))
    CuentaSaldo = int(input('Escribe el saldo de la cuenta'))
    CuentaServicios = set() #Contendrá los servicios a insertar
    Servicio = input('Escribe un servicio o deja vacio para parar')
    while (Servicio != ''):
        CuentaServicios.add(Servicio)
        Servicio = input('Escribe un servicio o deja vacio para parar')
    
    insertStatementTabla7 = session.prepare ('INSERT INTO Tabla7 (Cuenta_Numero, Cuenta_Servicios, Cuenta_Saldo) VALUES (?, ?, ?)')
    session.execute (insertStatementTabla7, [CuentaNumero, CuentaServicios, CuentaSaldo])
    
    insertStatementTabla8 = session.prepare ('INSERT INTO Tabla8 (Tarjeta_Limite, Cuenta_Numero, Tarjeta_Tipo, Tarjeta_Nombre) VALUES(?, ?, ?, ?)')
    session.execute(insertStatementTabla8, [TarjetaLimite, CuentaNumero, TarjetaTipo, TarjetaNombre])

#Función para insertar instancias de la relación CuBen (cuenta-beneficiario):
def insertCuBen ():
    BeneficiarioDNIPasaporte = input('Escribe la identificación del beneficiario')
    BeneficiarioNombre = input('Escribe el nombre del beneficiario')
    CuentaNumero = int(input('Escribe el número de cuenta'))
    CuentaSaldo = int(input('Escribe el saldo de la cuenta'))
    CuentaServicios = set() #Contendrá los servicios a insertar
    Servicio = input('Escribe un servicio o deja vacio para parar')
    while (Servicio != ''):
        CuentaServicios.add(Servicio)
        Servicio = input('Escribe un servicio o deja vacio para parar')
    
    insertStatementTabla7 = session.prepare ('INSERT INTO Tabla7 (Cuenta_Numero, Cuenta_Servicios, Cuenta_Saldo) VALUES (?, ?, ?)')
    session.execute (insertStatementTabla7, [CuentaNumero, CuentaServicios, CuentaSaldo])
                                            
    insertStatementTabla4 = session.prepare ('INSERT INTO Tabla4 (Beneficiario_DNIPasaporte, Cuenta_saldo) VALUES (?, ?) ')
    session.execute(insertStatementTabla4, [BeneficiarioDNIPasaporte, CuentaSaldo])

#Función que ejecuta un SELECT contra la base de datos y extrae la información de una sucursal según su Ciudad
def extraerDatosSucursal(Sucursal_Ciudad):
    select = session.prepare ("SELECT * FROM Tabla1 WHERE Sucursal_Ciudad = ?") 
    filas = session.execute (select, [Sucursal_Ciudad, ])
    for fila in filas:
        s = Sucursal (fila.Sucursal_Ciudad, fila.Sucursal_id, fila.Sucursal_Activo, fila.Sucursal_Nombre)
        return s

#Función que actualiza el estado de de una sucursal con base en la ciudad:
def actualizarEstadoSucursal ():
    Sucursal_Ciudad = input('Escribe la ciudad de la sucursal')
    Sucursal_Activo = input('Si está activa escribe cualquier letra, si no está activo oprime ENTER')
    infoSucursales = extraerDatosSucursal(Sucursal_Ciudad)
    if (infoSucursales != None): #Comprobar que la Sucursal_Ciudad este introducida en la BBDD, si no lo está, no ejecutamos ninguna operación.
        borrarSucursal = session.prepare ("DELETE FROM Tabla1 WHERE Sucursal_Activo = ? AND Sucursal_Ciudad = ?")
        session.execute(borrarSucursal, [infoSucursales.Sucursal_Activo, Sucursal_Ciudad])
        insertStatement = session.prepare("INSERT INTO Tabla1 (Sucursal_Ciudad, Sucursal_Activo, Sucursal_id, Sucursal_Nombre) VALUES (?, ?, ?, ?)")
        session.execute(insertStatement, [Sucursal_Ciudad, Sucursal_Activo, infoSucursales.Sucursal_id, infoSucursales.Sucursal_Nombre])
        update= session.prepare("UPDATE Tabla1 SET Sucursal_Activo = ? WHERE Sucursal_Ciudad = ?")
        session.execute(update, [Sucursal_Activo, Sucursal_Ciudad])
        
#Función que actualiza la ciudad de una sucursal con base en la id de la sucursal:
def actualizarCiudadSucursal ():
    Sucursal_Ciudad = input('Escribe la ciudad de la sucursal')
    Sucursal_id = input('Escribe la identificación de la sucursal')
    infoSucursales = extraerDatosSucursal(Sucursal_Ciudad)
    if (infoSucursales != None): #Comprobar que la Sucursal_Ciudad este introducida en la BBDD, si no lo está, no ejecutamos ninguna operación.
        borrarSucursal = session.prepare ("DELETE FROM Tabla1 WHERE Sucursal_Ciudad = ? AND Sucursal_id = ?")
        session.execute(borrarSucursal, [infoSucursales.Sucursal_Ciudad, Sucursal_id])
        insertStatement = session.prepare("INSERT INTO Tabla1 (Sucursal_Ciudad, Sucursal_id, Sucursal_Activo, Sucursal_Nombre) VALUES (?, ?, ?, ?)")
        session.execute(insertStatement, [Sucursal_Ciudad, Sucursal_id, infoSucursales.Sucursal_Activo, infoSucursales.Sucursal_Nombre])
        update= session.prepare("UPDATE Tabla1 SET Sucursal_Activo = ? WHERE Sucursal_Ciudad = ?")
        session.execute(update, [Sucursal_id, Sucursal_Ciudad])
        
#Funciones para las consultas de la actividad 1:

#Consulta1. Obtener toda la información de las sucursales de una ciudad específica
def extraerinfosucursalesciudad (Sucursal_Ciudad):
    select = session.prepare ('SELECT Sucursal_id, Sucursal_Nombre, Sucursal_Activo FROM Tabla1 WHERE Sucursal_Ciudad = ? ')
    filas = session.execute (select, [Sucursal_Ciudad, ])
    for fila in filas:
        s = Sucursal (Sucursal_Ciudad, fila.Sucursal_id, fila.Sucursal_Nombre, fila.Sucursal_Activo )
        return s

#Consulta2. Obtener el saldo que un cliente tiene en una cuenta en concreto (identificado por el número de cuenta).
def extraerinfosaldoclientecuenta (Cuenta_Numero):
    select = session.prepare ('SELECT Cuenta_Saldo, Cliente_DNIPasaporte FROM Tabla2 WHERE Cuenta_Numero = ? ')
    filas = session.execute (select, [Cuenta_Numero, ])
    for fila in filas:
        c = Cuenta (fila.Cuenta_Saldo, Cuenta_Numero, fila.Cliente_DNIPasaporte)
        return c
    
#Consulta3. Obtener con el nombre de un cliente, todas las sucursales donde tiene cuentas depositadas.
def extraercuentaclientenombre (Cliente_Nombre):
    select = session.prepare ('SELECT Sucursal_id, Cliente_DNIPasaporte, Cuenta_Numero FROM Tabla3 WHERE Cliente_Numero = ? ')
    filas = session.execute (select, [Cliente_Nombre, ])
    sucursales = []
    for fila in filas:
        s = Sucursal (Cliente_Nombre, fila.Sucursal_id, fila.Cliente_DNIPasaporte, fila.Cuenta_Numero)
        sucursales.append(s)
        return sucursales
        pass

#Consulta4. Obtener la cantidad de saldo que tiene un beneficiario con respecto a las cuentas de las que es beneficiario.
def extraersaldobeneficiario (Beneficiario_DNIPasaporte):
    select = session.prepare ('SELECT Cuenta_Saldo FROM Tabla4 WHERE Beneficiario_DNIPasaporte = ? ')
    filas = session.execute (select, [Beneficiario_DNIPasaporte, ])
    for fila in filas:
        c = Cuenta (Beneficiario_DNIPasaporte, fila.Cuenta_Saldo)
        return c

#Consulta5. Obtener con el nombre de una sucursal, todos los beneficiarios que tienen cuentas de las que son beneficiarios.
def extraercuentasbeneficiarios (Sucursal_Nombre):
    select = session.prepare ('SELECT Beneficiario_DNIPasaporte, Cuenta_Numero FROM Tabla5 WHERE Sucursal_Nombre = ? ')
    filas = session.execute (select, [Sucursal_Nombre, ])
    beneficiarios = []
    for fila in filas:
        b = Beneficiario (Sucursal_Nombre, fila.Beneficiario_DNIPasaporte, fila.Cuenta_Numero)
        beneficiarios.append(b)
        return beneficiarios
        pass

#Consulta6. Considerando que el 60% de los clientes viven en Barcelona, haga la tabla óptima según rendimiento en la que se consulte por la ciudad del cliente su información.
def extraerclienteciudad (Cliente_Ciudad):
    select = session.prepare ('SELECT Cliente_DNIPasaporte, Cliente_Nombre, Cliente_Calle FROM Tabla6 WHERE Cliente_Ciudad = ? ')
    filas = session.execute (select, [Cliente_Ciudad, ] )
    for fila in filas:
        c = Cliente (Cliente_Ciudad, filas.Cliente_DNIPasaporte, filas.Cliente_Nombre, filas.Cliente_Calle)
        return c

#Consulta7. Obtener los números de cuentas que tengan un servicio específico (por ejemplo, transferencias gratuitas).
def extraernumerocuentaservicio (Cuenta_Servicios):
    select = session.prepare ('SELECT Cuenta_Numero FROM Tabla7 WHERE Cuenta_Servicios = ? ')
    filas = session.execute (select, [Cuenta_Servicios, ])
    cuentas = []
    for fila in filas:
        c = Cuenta (fila.Cuenta_Numero, Cuenta_Servicios)
        cuentas.append(c)
        return cuentas
        pass

#Consulta8. Buscar según el límite salarial de una tarjeta las cuentas asociadas junto con el tipo y nombre de la tarjeta.
def extraercuentatiponombretar (Tarjeta_Limite):
    select = session.prepare ('SELECT Cuenta_Numero, Tarjeta_Tipo, Tarjeta_Nombre FROM Tabla8 WHERE Tarjeta_Limite = ? ')
    filas = session.execute (select, [Tarjeta_Limite, ])
    cuentas = []
    for fila in filas:
        c = Cuenta (Tarjeta_Limite, fila.Cuenta_Numero, fila.Tarjeta_Tipo, fila.Tarjeta_Nombre)
        cuentas.append(c)
        return cuentas
        pass

#Programa principal
#Conexión con Cassandra
cluster = Cluster()
#cluster = Cluster(['127.0.0.1', '1127.0.0.2'], port=..., ssl_context=...)
session = cluster.connect('diegopulido')
numero = -1
#Sigue pidiendo operaciones hasta que se introduzca 0
while (numero != 0):
    #Sigue pidiendo operaciones hasta que se introduzca 0
    while (numero != 0):
        print ("Introduzca un número para ejecutar una de las siguientes operaciones:")
        print ("1. Insertar una sucursal")
        print ("2. Insertar una cuenta")
        print ("3. Insertar un cliente")
        print ("4. Insertar relación entre cuenta y cliente (Depositante)")
        print ("5. Insertar relación entre tarjeta y cuenta (DetalleTar)")
        print ("6. Insertar relación entre cuenta y beneficiario (CuBen)")
        print ("7. Actualizar estado de una sucursal con base en la ciudad")
        print ("8. Actualizar la ciudad de una sucursal con base en la id de la sucursal")
        print ("9. Obtener toda la información de las sucursales de una ciudad específica")
        print ("10. Obtener el saldo que un cliente tiene en una cuenta en concreto (identificado por el número de cuenta)")
        print ("11. Obtener con el nombre de un cliente, todas las sucursales donde tiene cuentas depositadas")
        print ("12. Obtener la cantidad de saldo que tiene un beneficiario con respecto a las cuentas de las que es beneficiario")
        print ("13. Obtener con el nombre de una sucursal, todos los beneficiarios que tienen cuentas de las que son beneficiarios")
        print ("14. Consulta por la ciudad del cliente su información")
        print ("15. Obtener los números de cuentas que tengan un servicio específico (por ejemplo, transferencias gratuitas)")
        print ("16. Buscar según el límite salarial de una tarjeta las cuentas asociadas junto con el tipo y nombre de la tarjeta.")
        print ("0. Cerrar aplicación")
        
        numero = int (input()) #Pedimos numero al usuario
        if (numero == 1):
            insertSucursal()
        elif (numero == 2):
            insertCuenta()
        elif (numero == 3):
            insertCliente()
        elif (numero == 4):
            insertDepositante()
        elif (numero == 5):
            insertDetalleTar()
        elif (numero == 6):
            insertCuBen()
        elif (numero == 7):
            actualizarEstadoSucursal()
        elif (numero == 8):
            actualizarCiudadSucursal()
        elif (numero == 9):
            extraerinfosucursalesciudad()
        elif (numero == 10):
            extraerinfosaldoclientecuenta()
        elif (numero == 11):
            extraercuentaclientenombre()
        elif (numero == 12):
            extraersaldobeneficiario()
        elif (numero == 13):
            extraercuentasbeneficiarios()
        elif (numero == 14):
            extraerclienteciudad()
        elif (numero == 15):
            extraernumerocuentaservicio()
        elif (numero == 16):
            extraercuentatiponombretar()
        else:
            print ("Número incorrecto")
cluster.shutdown() #cerramos conexion

        





    
    
                                        
                                            
    
    
    
    
        
        
        
        



























        
        
        
        
        
        
