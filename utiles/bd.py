import mysql.connector

def conexionbd(sql):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="cesar",
        database="temperaturas"
    )

    mycursor = mydb.cursor()

    try:
        mycursor.execute(sql)

        # Si la consulta es un SELECT, obtén los resultados
        if sql.strip().lower().startswith('select'):
            result = mycursor.fetchall()
        else:
            result = None  # Para consultas que no devuelven resultados

        mydb.commit()

        return result
    except Exception as e:
        # Manejo de errores, por ejemplo, imprimir el error
        print(f"Error en la consulta: {e}")
        return None
    finally:
        mycursor.close()
        mydb.close()
