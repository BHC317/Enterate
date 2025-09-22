import logging

log = logging.getLogger(__name__)
logging.basicConfig(
    filename="myapp.log",
    format="{asctime} {levelname} {filename} - {message}", 
    style="{", 
    datefmt="%Y-%m-%d %H:%M:%S", 
    level=logging.INFO
)
