from flask import Flask, request, jsonify, render_template, Response
from flask_cors import CORS, cross_origin
import ast, json, time, argparse, datetime, operator
from functools import reduce
from datetime import datetime
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
CORS(app)

south = []
north = []
API_version = True
# status_table = {'south':[],'north':[],'barnB1':[],'barnB2':[]}
status_table = {'south':{},'north':{},'barnB1':{},'barnB2':{}}
updated_time = datetime.now()

# connecting to the database
mydb = mysql.connector.connect(
  host="fyp-database.cke3ogqxfbl8.ap-northeast-2.rds.amazonaws.com",
  user="root",
  passwd="fyppeoplecounterathkust",
    database="peoplecounter"
)
mycursor = mydb.cursor()


def clear_status_table():
    global status_table
    global updated_time

    if (datetime.now()-updated_time).days >= 1:
        status_table = {'south':{},'north':{},'barnB1':{},'barnB2':{}}


def realtime_stream(data, data_loc, entry_id):
    global status_table
    while True:
        j_data = None
        if data:
            # if len(status_table[data_loc])==0 or not entry_id in reduce(op/erator.concat, [list(t.keys()) for t in status_table[data_loc]]): # if entry_id is new client
            if len(status_table[data_loc])==0 or not entry_id in list(status_table[data_loc].keys()): # if entry_id is new client
                j_data = json.dumps(data)
                # status_table[data_loc].append({entry_id:False})
                status_table[data_loc][entry_id] = False
                print("[NEW CLIENT] ",entry_id)
            else:
                if status_table[data_loc][entry_id]:
                    j_data = json.dumps(data)
                    status_table[data_loc][entry_id] = False
                    print("[PUSH Data] ",entry_id)

        if type(j_data).__name__ == 'NoneType':
            print("\n[Yield Realtime data]")
            # upload the data to the database
            upload_to_database(json.loads(j_data))
            yield f"data:{j_data}\n\n"

        time.sleep(1)

def upload_to_database(data_list):
    def convert_datetime(datetime):
        y = datetime[6:10]
        m = datetime[0:2]
        d = datetime[3:5]
        h = datetime[12:14]
        mm = datetime[15:17]
        return f"{y}-{m}-{d} {h}:{mm}:00"
    for data in data_list:
        datetime = convert_datetime(data['datetime'])
        count = data['count']
        location = data['id']
        x1 = data['bbox'][0]
        y1 = data['bbox'][1]
        x2 = data['bbox'][2]
        y2 = data['bbox'][3]
        try:
            query = """INSERT INTO bounding_boxes (datetime, count, location, x1, y1, x2, y2)
                                VALUES (%s, %s, %s, %s, %s, %s, %s) """
            mycursor.executemany(query, [(datetime, count, location, x1, y1, x2, y2)])
        except mysql.connector.Error as error:
            print("Failed to insert into MySQL table {}".format(error))



@app.route('/', methods=['GET'])
def getRootPage():
    print("Main Page")
    if API_version:
        return {'Message':"Real-time People Detection"}
    else:
        return render_template("index.html")


@app.route('/init/south', methods = ['GET'])
def SouthInitHandler():
    global south
    status = 500
    clear_status_table()
    if request.method == 'GET':
        if API_version:
            return jsonify(data=south)
        else:
            if not south:
                return app.response_class(response=json.dumps({'message':'no received data'}),
                            status=status, mimetype='application/json')
            else:
                data = south[-1]
                return render_template("realtime_temp.html",
                id=data['id'], datetime=data['datetime'], count=data['count'],img_data=data['img_data'])


@app.route('/init/north', methods = ['GET'])
def NorthInitHandler():
    global north
    status = 500
    clear_status_table()
    if request.method == 'GET':
        if API_version:
            return jsonify(data=north)
        else:
            if not north:
                return app.response_class(response=json.dumps({'message':'no received data'}),
                            status=status, mimetype='application/json')
            else:
                data = north[-1]
                return render_template("realtime_temp.html",
                id=data['id'], datetime=data['datetime'], count=data['count'],img_data=data['img_data'])


@app.route('/realtime/south', methods = ['POST','GET'])
def SouthHandler():
    global south
    global status_table
    status = 500
    if request.method == 'GET':
        if API_version:
            # return jsonify(south)
            entry_id = request.remote_addr+"_"+str(datetime.datetime.now())
            return Response(realtime_stream(south, 'south', entry_id), mimetype='text/event-stream')
        else:
            if not south:
                return app.response_class(response=json.dumps({'message':'no received data'}),
                            status=status, mimetype='application/json')
            else:
                data = south[-1]
                return render_template("realtime_temp.html",
                id=data['id'], datetime=data['datetime'], count=data['count'],img_data=data['img_data'])

    if request.method == 'POST':
        if request.is_json:
            print("[Receive Request] len: {}".format(len(south)))
            if len(south) == 20:
                south.pop(0)
            south.append(request.get_json())
            keys = list(status_table['south'].keys())
            for k in keys:
                status_table['south'][k] = True
            status=200
        else:
            status=500

        return app.response_class(response=json.dumps({'id':'south'}),
                            status=status, mimetype='application/json')


@app.route('/realtime/north', methods = ['POST','GET'])
def NorthHandler():
    global north
    global status_table
    status = 500

    if request.method == 'GET':
        if API_version:
            # return jsonify(north)
            entry_id = request.remote_addr+"_"+str(datetime.datetime.now())
            return Response(realtime_stream(north,'north', request.remote_addr, entry_id), mimetype='text/event-stream')
        else:
            if not north:
                return app.response_class(response=json.dumps({'message':'no received data'}),
                            status=status, mimetype='application/json')
            else:
                data = north[-1]
                return render_template("realtime_temp.html",
                id=data['id'], datetime=data['datetime'], count=data['count'],img_data=data['img_data'])

    if request.method == 'POST':
        if request.is_json:
            print("[Receive Request] len: {}".format(len(north)))
            if len(north) == 20:
                north.pop(0)
            north.append(request.get_json())
            keys = list(status_table['north'].keys())
            for k in keys:
                status_table['north'][k] = True
            status=200
        else:
            status=500

        return app.response_class(response=json.dumps({'id':'north'}),
                            status=status, mimetype='application/json')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Web Server Parameters')
    parser.add_argument('--API_version', default=False,
                        help='Run as API server or html sender')
    args = parser.parse_args()
    API_version = args.API_version
    print("Run as API? ", API_version)

    app.run(host='0.0.0.0', port= 5000, threaded=True)

    print("AFTER APP RUN")
