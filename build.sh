echo oi 
mkdir dist
python3.12 -m venv env 
source env/bin/activate 
pip3.12 install --disable-pip-version-check --no-cache-dir -r requirements.txt
