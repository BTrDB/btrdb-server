package version

import "fmt"

const Major = 4
const Minor = 15
const Subminor = 8

var VersionString = fmt.Sprintf("%d.%d.%d", Major, Minor, Subminor)
var BuildDate string
