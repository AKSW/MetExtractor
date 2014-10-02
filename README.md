# MetEx – Metadata Extractor
This is a project for extracting metadata for a list of SPARQL endpoints/services from various services.

## Run instructions
To run this programm you will need a configuration file in turtle. This file should describe a configuration resource, which is selected as run resource.

## Requirements

  * [Virtuoso Jena Bindings](http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VOSDownload#Jena%20Provider) see also the [Virtuoso documentation about this topic](http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtJenaProvider)
  * [Sindice sparql-summary](https://github.com/sindice/sparqled/tree/wims/sparql-summary) (wims branch)
  * [Java Linked Data Helper](https://gitlab.deri.ie/white-gecko/javalinkeddatahelper)
  * [JSON-RPC Client](http://software.dzhuvinov.com//download.html#download-jsonrpc2client) and [JSON-RPC Base](http://software.dzhuvinov.com//download.html#download-jsonrpc2base), see also http://software.dzhuvinov.com/json-rpc-2.0-client.html
  * [JSON-Smart](http://code.google.com/p/json-smart/)

## License
Copyright (C) 2014  Natanael Arndt

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA, or see
[http://www.gnu.org/licenses/gpl-2.0.html](http://www.gnu.org/licenses/gpl-2.0.html)
for more details.
