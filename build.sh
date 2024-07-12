pip install -r requirements.txt

find node_modules -type d -name "test" -exec rm -rf {} +
find node_modules -type d -name "examples" -exec rm -rf {} +
find node_modules -type d -name "docs" -exec rm -rf {} +

du -sh *
