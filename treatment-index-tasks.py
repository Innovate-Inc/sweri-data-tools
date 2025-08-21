from worker import app


@app.task()
def fetch_features():
    pass