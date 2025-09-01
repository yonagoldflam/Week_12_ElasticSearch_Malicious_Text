import uvicorn as uv
from fastapi import FastAPI
from elastic.manager import Manager

app = FastAPI()
manager = Manager()


@app.get('/data')
def get_data():
    return manager.find_antisemitic_weapons()

@app.get('/weapons')
def get_weapons():
    return manager.find_least_2_weapons()

if __name__ == '__main__':
    uv.run('main:app', host='127.0.0.1', port=8000)