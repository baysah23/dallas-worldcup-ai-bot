from app import app as application

# Alias (some platforms expect `app`)
app = application

# Marker for CI hard-gate
application.config["WSGI_LOADED"] = True