from celery import Celery

sqlite_filename = "sqlite:///./.local/dwm-celery.dev.db"

app = Celery("cmdbus", broker='sqla+' + sqlite_filename, backend='db+' + sqlite_filename,)

@app.task
def helloworld():
    print('hello world', flush=True)
