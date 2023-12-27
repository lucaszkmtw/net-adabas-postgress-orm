# Readme

## Install jython

``` bash
sudo apt install default-jre jython
dirname $(dirname $(readlink -f $(which javac)))
pip install -r requirements.txt
```


## configuracion de postgres

``` bash
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres:13.2-alpine
```
