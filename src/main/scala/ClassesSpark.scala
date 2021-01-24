import java.time.LocalDateTime

case class ClassesSpark()
case class Station(
                    stationId:Integer,
                    name:String,
                    lat:Double,
                    long:Double,
                    dockcount:Integer,
                    landmark:String,
                    installation:String,
                    notes:String)

case class Trip(
                 tripId:Integer,
                 duration:Integer,
                 startDate:LocalDateTime,
                 startStation:String,
                 startTerminal:Integer,
                 endDate:LocalDateTime,
                 endStation:String,
                 endTerminal:Integer,
                 bikeId: Integer,
                 subscriptionType: String,
                 zipCode: String)
