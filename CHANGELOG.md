# Changelog

## [0.9.1](https://github.com/reanahub/reana-dask-kubernetes-operator/compare/v0.9.0...0.9.1) (2026-06-07)


### Build

* **python:** relax version constraints for shared REANA modules ([#3](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/3)) ([6f53792](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/6f53792379f683ab95a1803947ddefb5afee13b1))


### Features

* **watcher:** add initial version ([#1](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/1)) ([c3b49f9](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/c3b49f9520e49c25fb449116662cfff623582ff9))


### Bug fixes

* **config:** use glob for egg-info in flake8 exclude ([#12](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/12)) ([9a9b9c8](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/9a9b9c8a381766875faa6c58393b25645a6803c6))
* **dask:** persist service logs on pod termination ([#8](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/8)) ([49f2b3e](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/49f2b3ee75ed7fbf533757838ebabe70aa19d82c))
* **git:** update .gitignore to exclude modules directory ([#13](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/13)) ([4bb7e93](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/4bb7e93f70bc39e162f2045dacb1817239511523))
* **watcher:** bypass kubernetes 36 pod log deserialiser ([#14](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/14)) ([38c05d3](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/38c05d3db7cc18295d6083495a883a11a333a1ea))


### Code style

* **python:** adapt code base to the new black version ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([ec4497d](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/ec4497d075e5f8b3fdc1b9ea5810088691629eb9))


### Continuous integration

* **commitlint:** fix local running of commit linter on macOS ([#2](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/2)) ([90482fd](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/90482fd0e76bb53d629d683a5b6b566220428cfe))
* **jsonlint:** add JSON linting checks ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([86ca9d2](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/86ca9d2376d6eb88916ad66df201fec6b274d8c4))
* **markdownlint:** add Markdown linting checks ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([0cc2ef7](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/0cc2ef7148817e68a3fc5ed0cd2541412d044bae))
* **prettier:** add Prettier code formatting checks ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([8db2c05](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/8db2c055fb019408dc8d74e6fd56e6acc1633add))
* **python:** pin setuptools below 81 ([#5](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/5)) ([c35a5e2](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/c35a5e29679db55fb8814332950aec687eeedc8d))
* **release-please:** correct manifest filename typo ([#10](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/10)) ([0f3d7d4](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/0f3d7d46905253f1a0d0ec1c015596d43c1138d7))
* **run-tests:** add usage help and refactor options ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([26526e5](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/26526e5faec8d1957a61f399180eb2af3bb21783))
* **run-tests:** correct package name for pydocstyle and docker ([#9](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/9)) ([f5a48fe](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/f5a48fef9a497839af077bcee6709a5b4d5cdb0e))
* **shfmt:** add shfmt code formatting checks ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([f6b4ffa](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/f6b4ffa95a3a006193cd1d3a1ee743cb43105c5f))
* **yamllint:** add YAML linting checks ([#6](https://github.com/reanahub/reana-dask-kubernetes-operator/issues/6)) ([075796a](https://github.com/reanahub/reana-dask-kubernetes-operator/commit/075796a7acb64e996f6fef4fe5439a8ad7edc33f))
