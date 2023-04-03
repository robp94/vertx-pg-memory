if ! (("$1" % 500)); then
    echo "$1"
fi
curl -s --location "http://localhost:8080/test/leak" > /dev/null