from flask import Flask, jsonify
from flask_cors import CORS
import elasticsearch
import urllib3
from flask import render_template

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

username = 'elastic'
password = 'Burek123'

es = elasticsearch.Elasticsearch(
    ['https://localhost:9200'],
    basic_auth=(username, password),
    verify_certs=False
)

app = Flask(__name__)

# Omogočimo CORS za vse poti
CORS(app)

@app.route('/')
def index():
    message =  'Hello, World!'
    return render_template('index.html', message=message)

@app.route('/get_total_count')
def get_total_count():
    # Poizvedba za iskanje besede "SPLOŠNA" v polju DELOVNO_MESTO
    query = {
        "query": {
            "wildcard": {
                "DELOVNO_MESTO": {
                    "value": "*DR*"
                }
            }
        }
    }

    # Pridobi število vseh zadetkov
    count_response = es.count(index="mt-sevanje", body=query)
    total_count = count_response['count']

    return jsonify({"total_count": total_count})

if __name__ == '__main__':
    app.run(port=8080, debug=True)