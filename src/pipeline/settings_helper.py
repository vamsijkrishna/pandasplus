# settings_helper.py

def get_var(config, name, optional=False, default=None):
    
    for n in name:

        if not n in config:
            if not optional:
                raise Exception("%s not in configuration!" % (name))
            else:
                return default
        tmp = config[n]

    return tmp