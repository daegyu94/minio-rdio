package cmd

import (
    "log"
    "time"
    "runtime"
)

var timeTrackOn = false

func getFunctionName() string {
    pc, _, _, _ := runtime.Caller(1) /* 1st parent function */
    fn := runtime.FuncForPC(pc)
    return fn.Name()
}

func timeTrack(start time.Time, name string) {
    elapsed := time.Since(start)
    log.Printf("%s took %s", name, elapsed)
}

/*
// usage example
func do_something() {
    if timeTrackOn { defer timeTrack(time.Now(), getFunctionName()) }
    r := new(big.Int)
    r.Binomial(1000, 10)
}
*/
