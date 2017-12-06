# LAKEsuperior

LAKEsuperior is an experimental [Fedora Repository](http://fedorarepository.org)
implementation.

## Basic concepts

LAKEsuperior aims at being an uncomplicated, efficient Fedora 4 implementation.

Implementation of the official Fedora API specs (Fedora 5.x and beyond) is not
foreseen in the short term, however it may become a natural evolution of this
project if it gains support.

Please make sure you read the [Delta document](doc/notes/fcrepo4_deltas) for
divergences with the official Fedora4 implementation.

Alpha 2 consists of less than 2200 lines of code and strives to maintain a
linear, intuitive code structure to foster collaboration. *TODO link to tech
overview and approach*

## Installation

### Dependencies

1. A triplestore.
   [Fuseki](https://jena.apache.org/documentation/fuseki2/#download-fuseki)
   is the benchmark used so far in development. Other implementations are
   possible as long as they support RDF 1.1 and SPARQL over HTTP
1. A message broker supporting the STOMP protocol. If you have a separate
   instance of official Fedora listening to port 61613, that will do the job
1. Python 3.5 or greater

### Installation steps

1. Install dependencies as indicated above
1. Create a virtualenv in a project folder:
   `virtualenv -p <python 3.5+ exec path> <virtualenv folder>`
1. Initialize the virtualenv: `source <path_to_virtualenv>/bin/activate`
1. Clone this repo
1. `cd` into repo folder
1. Install dependencies: `pip install -r requirements.txt`
1. Copy the `etc.skeleton` folder to a separate location
1. Set the configuration folder location in the environment:
   `export FCREPO_CONFIG_DIR=<your config dir location>` (alternatively you can
   add this line to your virtualenv `activate` script)
1. Configure the application
1. Start your triplestore and STOMP broker
1. Run `util/bootstrap.py` to initialize the binary and graph stores
1. Run `./fcrepo` for a multi-threaded server or `flask run` for a
   single-threaded development server

### Production deployment

If you like fried repositories for lunch, deploy before 11AM.

## Status and development

LAKEsuperior is in **alpha** status. Please see the [TODO](doc/notes/TODO) list
for a rudimentary road map and status.

## Further documentation

The design documents are in the [doc/pdf](doc/pdf) folder. *@TODO needs update*
