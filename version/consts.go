package version

import "fmt"

const Major = 4
const Minor = 12
const Subminor = 0

var VersionString = fmt.Sprintf("%d.%d.%d", Major, Minor, Subminor)
var BuildDate string
