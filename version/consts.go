package version

import "fmt"

const Major = 4
const Minor = 7
const Subminor = 3

var VersionString = fmt.Sprintf("%d.%d.%d", Major, Minor, Subminor)
var BuildDate string
