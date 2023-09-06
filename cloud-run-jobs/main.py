import task
from flask import Flask

app = Flask("myapp")

@app.get("/mytask")
def index():
    res = task.compute()
    return res

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080)