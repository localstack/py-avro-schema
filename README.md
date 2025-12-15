# LocalStack py-avro-schema

This project is built on [py-avro-schema](https://github.com/jpmorganchase/py-avro-schema).

## Developing

To setup a virtual environment under `.venv/`, first install `uv`, then run:

```shell
make install-dev
```

## Release a new version

To release a new version you can create a tag with the following commands:

```shell
git tag v4.0.0
git push origin v4.0.0
```

This will create a git tag and push it to the remote repository.
This will trigger the build workflow and publish the package to PyPI.
