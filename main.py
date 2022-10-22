import os
from markupsafe import escape
from app import app, upload_video
from flask import request, render_template

BUCKET_NAME = os.getenv('BUCKET_NAME')


@app.get('/')
def index():
    return render_template('index.html', bucket_name=BUCKET_NAME)


@app.post('/')
def upload():
    # link can be supplied through the form or a direct POST request
    request.get_data()
    youtube_link = escape(request.form.get('youtube-link', request.data.decode('UTF-8')))
    return upload_video(youtube_link, BUCKET_NAME)

if __name__ == '__main__':
    app.run(debug=True)

