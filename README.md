phovea_data_opencpu [![Phovea][phovea-image]][phovea-url] [![NPM version][npm-image]][npm-url] [![Build Status][travis-image]][travis-url] [![Dependency Status][daviddm-image]][daviddm-url]
=====================

A phovea data provider plugin for accessing R objects using [OpenCPU](http://opencpu.org/)

Configuration
 ------------
```json
{
  "host": "ocpu",
  "port": 8004,
  "sessions": [
    {
      "name": "survey",
      "script": "library(MASS); survey = survey",
      "meta": {
        "survey": {
          "idtype": "Student"
        }
      }
    }
  ]
}
```

 * host / port ... where the OpenCPU cluster lives. 
 * script ... the session initialization script. Objects within the sessions are detected during startup and are available in Phovea
 * meta ... additional meta data not included in the R objects, e.g., the idtype of individual data structures


Installation
------------

```
git clone https://github.com/sgratzl/phovea_data_opencpu.git
cd phovea_data_opencpu
npm install
```

Testing
-------

```
npm test
```

Building
--------

```
npm run build
```



***

<a href="https://caleydo.org"><img src="http://caleydo.org/assets/images/logos/caleydo.svg" align="left" width="200px" hspace="10" vspace="6"></a>
This repository is part of **[Phovea](http://phovea.caleydo.org/)**, a platform for developing web-based visualization applications. For tutorials, API docs, and more information about the build and deployment process, see the [documentation page](http://phovea.caleydo.org).


[phovea-image]: https://img.shields.io/badge/Phovea-Server%20Plugin-10ACDF.svg
[phovea-url]: https://phovea.caleydo.org
[npm-image]: https://badge.fury.io/js/phovea_data_opencpu.svg
[npm-url]: https://npmjs.org/package/phovea_data_opencpu
[travis-image]: https://travis-ci.org/sgratzl/phovea_data_opencpu.svg?branch=master
[travis-url]: https://travis-ci.org/sgratzl/phovea_data_opencpu
[daviddm-image]: https://david-dm.org/sgratzl/phovea_data_opencpu/status.svg
[daviddm-url]: https://david-dm.org/sgratzl/phovea_data_opencpu
