import os
# Marker set BEFORE importing app.py so production guard can verify WSGI path.
os.environ["WCG_WSGI"] = "1"

from app import app as application

app = application
application.config["WSGI_LOADED"] = True
