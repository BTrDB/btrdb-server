#!/usr/bin/env python

f = open("swagger.json.go","w")
f.write("package httpinterface\n")
swag = open("btrdb.swagger.json").read()
f.write("const SwaggerJSON = `")
f.write(swag)
f.write("`;")
f.close()
