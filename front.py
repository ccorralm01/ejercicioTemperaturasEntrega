import main
from flask import Flask, render_template, request, jsonify
from datetime import timedelta

app = Flask(__name__, static_url_path='/static')

@app.route('/')
def principal():
    return render_template('index.html', progreso_carga=main.progreso_carga)

@app.route('/buscar')
def buscar():
    pais = request.args.get('pais')
    pais_data = main.recuperar_temperaturas(pais)

    # Convertir timedelta a formato HH:MM:SS
    for data in pais_data:
        data['amanecer'] = str(data['amanecer'])
        data['atardecer'] = str(data['atardecer'])

    return jsonify(pais_data)

@app.route('/progreso')
def obtener_progreso():
    return jsonify({'progreso': main.progreso_carga})

if __name__ == '__main__':
    app.run(debug=True)
