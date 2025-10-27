import configparser

config = configparser.ConfigParser()
path = 'E:\demoproject\configs\\application.conf'
env = 'LOCAL'
read_files = config.read(path)
if not read_files:
    raise FileNotFoundError(f"Configuration file not found at path: {path}")
else :
    app_conf = {}
    for key, value in config.items(env):
        app_conf[key] = value
    print(app_conf)

