package amplab.schema {

  case class Ranking (
    pageURL:     String,
    pageRank:    Int, 
    avgDuration: Int 
  )

  case class UserVisit (
    sourceIP:     String,
    destURL:      String,
    visitDate:    String,
    adRevenue:    Double,
    userAgent:    String,
    countryCode:  String,
    languageCode: String,
    searchWord:   String,
    duration:     Int
  )

}
