# Scripts for Orenza web server DB update/creation

A pipeline to create/update the ORENZA database in https://bioi2.i2bc.paris-saclay.fr/django/orenza/

## Usage

Created for use on the I2BC cluster but easily adaptable. The main script is `update.py` with a wrapper for the I2BC cluster: `update.sh`

```bash
python3 update.py all
```

or
```bash
python3 update.sh
```
