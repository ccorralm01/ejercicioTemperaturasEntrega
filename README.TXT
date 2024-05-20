En el ejercicio he utilizado Flask como nos recomendaste, la aplicacion se ejecuta
desde el archivo Front.py (python front.py).

En la raíz tienes el fichero front que se encarga de la ejecución de Flask y el
fichero main que se encarga de la funcionalidad de la aplicación. En main están las
funciones explicadas con comentarios.

Al insetar temperaturas en la BD tarda un poco en responder la aplicación porque hace bastantes
inserciones a si que he añadido una barra de progreso en la parte de arriba de la página
web.

Dentro de la carpeta "utiles" tienes los ficheros de conexión a la base de datos y
a las dos APIS.

En la carpeta templates esta el HTML que se muestra en la web al cual le añadí una
librería de JavaScript (three.js) para dejar el front más original y bonito. 

En la carpeta static hay algunos recursos (imagenes) que utiliza la aplicación.

En esta web esta el trabajo implementado: https://thekingkraken.duckdns.org
aunque en la implementación en la web la barra de progreso parpadea.
