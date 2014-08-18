__author__ = 'immesys'

import numpy as np
import qdf
from twisted.internet import defer

class Example1HZ(qdf.QuasarDistillate):

    def setup(self, opts):
        """
        This constructs your distillate algorithm
        """
        #This is the first level in the distillate tree
        self.set_author("MPA")

        #This is the second level. This name should be unique for every algorithm you write
        self.set_name("SinWaves")

        #This is the final level. You can have multiple of these
        self.add_stream("1hz", unit="unit")
        self.add_stream("2hz", unit="unit")

        #If this is incremented, it is assumed that the whole distillate is invalidated, and it
        #will be deleted and discarded. In addition all 'persist' data will be removed
        self.set_version(7)

    @defer.inlineCallbacks
    def compute(self):
        """
        This is called to compute your algorithm.

        This example generates some sin waves at different frequencies. It also ensures that this
        sin wave extends from Aug 17th until the present time
        """
        #Typically, a stream would be based upon other streams, so you would calculate the ranges
        #that need updating by querying the changed ranges in those streams. We are generating
        #artificial streams, so we cannot do that. Instead, we use the persistent data store to
        #work out when the end date was the last time compute() was run.
        #The first parameter is the name of the data point we stored. The second is what value
        #to get back if it did not exist.
        last_end_date = self.unpersist("end_timestamp", None)
        if last_end_date is None:
            #this is the first time we have run this algorithm or we
            #incremented the version number
            last_end_date = self.date("2014-08-17T00:00:00") #An example of the date formats we use

        timestamp = last_end_date

        values_1hz = []
        values_2hz = []
        while timestamp < self.now():
            #Round down to the second to prevent cumulative error
            timestamp /= 1000000000
            timestamp *= 1000000000
            for i in xrange(120):
                delta = 8333333 #This corresponds with the one used by the uPMUs
                values_1hz.append((timestamp + delta*i, np.sin( 2*np.pi   * i*delta/1E9)))
                values_2hz.append((timestamp + delta*i, np.sin( 2*2*np.pi * i*delta/1E9)))

            if len(values_1hz) >= qdf.OPTIMAL_BATCH_SIZE:
                yield self.stream_insert_multiple("1hz", values_1hz)
                yield self.stream_insert_multiple("2hz", values_2hz)
                values_1hz = []
                values_2hz = []

            timestamp += 1000000000 #Next second

        #Make sure that we write out the values we generated
        yield self.stream_insert_multiple("1hz", values_1hz)
        yield self.stream_insert_multiple("2hz", values_2hz)

        #Now that we are done, save the time we finished at
        self.persist("end_timestamp", timestamp)


qdf.register(Example1HZ())
qdf.begin()
