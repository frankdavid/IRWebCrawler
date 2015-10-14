package ch.ethz.ir

case class CrawlResult(numDistinctUrls: Int,
                       numExactDuplicates: Int,
                       numNearDuplicates: Int,
                       numEnglishPages: Int,
                       studentFrequency: Int)
