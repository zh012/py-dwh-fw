import os
if not os.getenv('ROOT_PATH_FOR_DYNACONF'):
    os.environ['ROOT_PATH_FOR_DYNACONF'] = os.path.dirname(__file__)

from dynaconf import settings
