package version

import "fmt"

const Major = 4
const Minor = 4
const Subminor = 9

//Will be set at build time to Major.Minor.Build
var VersionString = fmt.Sprintf("%d.%d.%d", Major, Minor, Subminor)
var BuildDate string
