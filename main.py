from utiles import apis, bd
import json
import xml.etree.ElementTree as ET
import datetime

# Funcion que extrae los datos de los paises de la api y los inserta en la BD
def cargar_paises_bd():
    consulta = apis.consulta_api_paises("region/europe")

    # Cargamos los datos de la api
    json_data = json.loads(consulta)

    # Por cada país sacamos los datos que necesitamos del JSON
    for country_info in json_data:
        pais = []
        nombre = country_info["name"]["common"].lower()
        capital = country_info["capital"][0].lower() if "capital" in country_info else "N/A"
        region = country_info["region"].lower() if "region" in country_info else "N/A"
        subregion = country_info["subregion"].lower() if "subregion" in country_info else "N/A"
        cca2 = country_info["cca2"].lower()
        cca3 = country_info["cca3"].lower()
        miembroue = str(country_info["unMember"]) if "unMember" in country_info else "false"
        latlng = [str(coord).lower() for coord in country_info.get("latlng", ["N/A", "N/A"])]
        latlng = country_info.get("latlng", ["N/A", "N/A"])
        sql_paises = (f'INSERT INTO paises (cca2, cca3, nombre, capital, region, subregion, '
                      f'miembroUE, latitud, longitud) VALUES ("{cca2}", "{cca3}", "{nombre}", "{capital}", '
                      f'"{region}", "{subregion}", "{miembroue}", "{latlng[0]}", "{latlng[1]}")')
        sql_comprobar_duplicados = f"SELECT * FROM PAISES WHERE nombre = '{nombre}'"

        # Una vez los tenemos los insertamos en la BD
        if not (bd.conexionbd(sql_comprobar_duplicados)):
            bd.conexionbd(sql_paises)

# Funcion que relaciona el id asignado por la BD y sus países fronterizos
def cargar_fronteras_bd():
    consulta = apis.consulta_api_paises("region/europe")
    json_data = json.loads(consulta)

    # Cargamos los paises asociados a un ID que pedimos a la BD
    for country_info in json_data:
        nombre = country_info["name"]["common"]
        sql_id_pais = f'SELECT idpais FROM paises where nombre = "{nombre}"'
        borders = country_info.get("borders", [])
        id_pais = int((bd.conexionbd(sql_id_pais)[0])[0])
        for cca3 in borders:
            sql_comprobar_duplicados_fronteras = (f'SELECT idpais, cca3_frontera FROM fronteras WHERE '
                                                  f'idpais="{id_pais}" AND cca3_frontera = "{cca3}"')
            sql_fronteras = f'INSERT INTO fronteras (idpais, cca3_frontera) VALUES ("{id_pais}", "{cca3.lower()}")'
            if not (bd.conexionbd(sql_comprobar_duplicados_fronteras)):
                bd.conexionbd(sql_fronteras)


progreso_carga = 0
# Como el insertar temps en la BD tarda mucho implementé una barra de progreso
def actualizar_progreso(progreso):
    global progreso_carga
    progreso_carga = progreso


# Funcion que extrae los datos metereologicos de los paises por lat y long y los guarda en BD
def cargar_temperaturas():
    sql_numero_paises = "SELECT COUNT(*) FROM `paises`;"
    n_paises = int((bd.conexionbd(sql_numero_paises)[0])[0])
    paises_xml = round(n_paises/2)
    paises_json = n_paises-paises_xml

    sql_latlon_pais = "SELECT `idpais`, `latitud`, `longitud` FROM `paises`;"
    latlon_paises = bd.conexionbd(sql_latlon_pais)
    typeres = ""

    # Extraer mitad y mitad JSON, XML

    for index, latlon_pais in enumerate(latlon_paises):
        if (index+1) <= paises_json:
            typeres = "JSON"
        else:
            typeres = "XML"

        pais_data_bytes = apis.consulta_api_temperaturas(typeres, latlon_pais)

        # Decodificar la cadena de bytes a una cadena de texto
        pais_data_str = pais_data_bytes.decode('utf-8')

        if typeres == "JSON":
            pais_data = json.loads(pais_data_str)
            temperatura = pais_data['main']['temp']
            sensacion = pais_data['main']['feels_like']
            temp_minima = pais_data['main']['temp_min']
            temp_maxima = pais_data['main']['temp_max']
            humedad = pais_data['main']['humidity']
            amanecer = datetime.datetime.utcfromtimestamp(pais_data['sys']['sunrise'])
            atardecer = datetime.datetime.utcfromtimestamp(pais_data['sys']['sunset'])

        else:
            root = ET.fromstring(pais_data_str)
            temperatura = root.find('.//temperature').attrib['value']
            temp_minima = root.find('.//temperature').attrib['min']
            temp_maxima = root.find('.//temperature').attrib['max']
            sensacion = root.find('.//feels_like').attrib['value']
            humedad = root.find('.//humidity').attrib['value']

            amanecer = root.find('.//sun').attrib['rise']
            atardecer = root.find('.//sun').attrib['set']

        # conversion a Cº
        temperatura = float(temperatura) - 273.15
        temp_minima = float(temp_minima) - 273.15
        temp_maxima = float(temp_maxima) - 273.15
        sensacion = float(sensacion) - 273.15
        '''print(index + 1,
              "idpais: ", latlon_pais[0],
              "// temperatura:", temperatura,
              "// sensacion:", sensacion,
              "// TempMin:", temp_minima,
              "// TempMax:", temp_maxima,
              "// Humedad:", humedad,
              "// amanecer:", hora_amanecer,
              "// atardecer:", hora_atardecer
              )'''
        timestamp_actual = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql_temperaturas = (
            f"INSERT INTO `temperaturas`(`idpais`, `timestamp`, `temperatura`, `sensacion`, `minima`, "
            f"`maxima`, `humedad`, `amanecer`, `atardecer`) "
            f"VALUES('{latlon_pais[0]}', '{timestamp_actual}', '{temperatura}', '{sensacion}', '{temp_minima}', "
            f"'{temp_maxima}', '{humedad}', '{amanecer}', '{atardecer}')"
        )

        bd.conexionbd(sql_temperaturas)

        progreso = int((index + 1) / len(latlon_paises) * 100)
        actualizar_progreso(progreso)

    return

# Funcion que carga todos los datos en la BD
def cargar_datos_en_bd():
    cargar_paises_bd()
    cargar_fronteras_bd()
    cargar_temperaturas()


# Función que recupera los datos guardados del país y sus fronteras en la tabla temperaturas
def recuperar_temperaturas(pais):
    cargar_datos_en_bd()
    sql_recuperar_tmp_pais_y_fronteras = (
        f"SELECT p.nombre AS nombre_pais, temp_table.idpais, temp_table.temperatura, temp_table.sensacion, "
        f"temp_table.minima, temp_table.maxima, temp_table.humedad, temp_table.amanecer, temp_table.atardecer "
        f"FROM ("
        f"  SELECT t.idpais, t.timestamp, t.temperatura, t.sensacion, t.minima, t.maxima, t.humedad, t.amanecer, t.atardecer, "
        f"         ROW_NUMBER() OVER (PARTITION BY t.idpais ORDER BY t.timestamp DESC) AS row_num "
        f"  FROM temperaturas t "
        f"  JOIN paises p ON t.idpais = p.idpais "
        f"  WHERE p.nombre = '{pais}' OR p.cca3 IN (SELECT cca3_frontera FROM fronteras WHERE idpais = (SELECT idpais FROM paises WHERE nombre = '{pais}'))"
        f") AS temp_table "
        f"JOIN paises p ON temp_table.idpais = p.idpais "
        f"WHERE row_num = 1;"
    )

    result = bd.conexionbd(sql_recuperar_tmp_pais_y_fronteras)
    data = []
    for row in result:
        data.append({
            'nombre_pais': row[0],
            'id_pais': row[1],
            'temperatura': row[2],
            'sensacion': row[3],
            'minima': row[4],
            'maxima': row[5],
            'humedad': row[6],
            'amanecer': row[7],
            'atardecer': row[8],
        })

    return data

