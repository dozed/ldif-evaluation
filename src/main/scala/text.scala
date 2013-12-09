import com.wcohen.ss.api.{StringWrapper, Token, Tokenizer}
import com.wcohen.ss._
import com.wcohen.ss.tokens.BasicToken
import java.io.{Serializable, FileInputStream, Reader, StringReader}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.en.PorterStemFilter
import org.apache.lucene.analysis.miscellaneous.LengthFilter
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.{FilteringTokenFilter, CharTokenizer}
import org.apache.lucene.analysis.{TokenStream, Analyzer}
import org.apache.lucene.util.Version
import scala.collection.JavaConverters._

object analyze {

  /**
   * analyzer
   *
   * - tokenizer rules
   * separator: ,.()'/
   * no-separator: –-&!*+:
   * alternative: split also at –-
   *
   * - filter
   * length filter: <= 1
   * numeric filter
   * filter tokens which consist only of -
   *
   * - stop word filter
   * none, use weights instead
   * maybe use a very basic stop word filter, since few training data is available
   * internal training vs. externally trained weights
   *
   * - stemming: none
   *
   * - synonyms
   *
   * - add parenthesis information, could be useful for weighting
   *
   */

  case class ExtCharTokenizer(in: Reader)(implicit v: Version) extends CharTokenizer(v, in) {
    protected def isTokenChar(c: Int): Boolean = {
      !(Character.isWhitespace(c) || c == ',' || c == '.' || c == '(' || c == ')' || c == '\'' || c == '/')
    }
  }

  case class ExtNumericFilter(in: TokenStream)(implicit v: Version) extends FilteringTokenFilter(v, in) {
    private val termAtt = addAttribute(classOf[CharTermAttribute])

    def accept: Boolean = {
      termAtt.buffer.take(termAtt.length) exists (c => !Character.isDigit(c))
    }
  }

  case class ExtSpecialCharacterFilter(in: TokenStream)(implicit v: Version) extends FilteringTokenFilter(v, in) {
    private val termAtt = addAttribute(classOf[CharTermAttribute])

    def accept: Boolean = {
      termAtt.buffer.take(termAtt.length) exists (c => c != '–' && c != '-')
    }
  }

  case class ExtAnalyzer(implicit matchVersion: Version) extends Analyzer {
    protected def createComponents(fieldName: String, reader: Reader): TokenStreamComponents = {
      val source = ExtCharTokenizer(reader)
      // val source = new ClassicTokenizer(matchVersion, reader)

      val stemmer = new PorterStemFilter(source)

      val filter = new ExtSpecialCharacterFilter(
        new ExtNumericFilter(
          new LengthFilter(matchVersion,
            new LowerCaseFilter(matchVersion, source), 2, Integer.MAX_VALUE)))

      new TokenStreamComponents(source, filter)
    }
  }

  implicit val matchVersion = Version.LUCENE_46
  val analyzer = ExtAnalyzer()

  def isPunctuation(ch: Char): Boolean = {
    !(Character.isWhitespace(ch) || Character.isLetter(ch) || Character.isDigit(ch))
  }

  def tokenize(s: String): List[String] = {
    val ts = analyzer.tokenStream("myfield", new StringReader(s))
    val termAtt = ts.addAttribute(classOf[CharTermAttribute])
    val tkns = collection.mutable.ArrayBuffer[String]()

    try {
      ts.reset
      while (ts.incrementToken) {
        tkns += termAtt.toString
      }
      ts.end
    } finally {
      ts.close
    }

    tkns.toList
  }

  def tokenizeOpt(s: String): Option[List[String]] = {
    val t = tokenize(s)
    if (t.length > 0) Some(t) else None
  }

  def labelLengthsDistribution(labels: Map[String, String]): List[(Int, Int)] = {
    val labelLengths = collection.mutable.Map[Int, Int]()
    labels.values foreach {
      l =>
        val tkns = tokenize(l)
        labelLengths(tkns.length) = labelLengths.getOrElse(tkns.length, 0) + 1
    }
    labelLengths.toList.sortBy(_._1)
  }

  def documentFrequencies(labels: List[String]) = {

  }

  /**
   * An interned version of a string.
   *
   */
  case class ExtBasicToken(index: Int, value: String) extends Token with Comparable[ExtBasicToken] with Serializable {

    def getValue = value

    def getIndex = index

    def compareTo(o: ExtBasicToken): Int = {
      val t: Token = o.asInstanceOf[Token]
      return index - t.getIndex
    }

    override def hashCode = value.hashCode

    override def toString = f"[tok $index: $value]"

    override def equals(t: scala.Any): Boolean = {
      if (t.isInstanceOf[BasicToken]) return this.hashCode == (t.hashCode)
      return super.equals(t)
    }

  }

  case class ExtTokenizerSs(ignoreCase: Boolean = true) extends Tokenizer {

    def tokenize(input: String): Array[Token] = {
      analyze.tokenize(input) map intern toArray
    }

    private def internSomething(s: String): Token = {
      intern(if (ignoreCase) s.toLowerCase else s)
    }

    private var nextId = 0
    private val tokMap = collection.mutable.Map[String, Token]()

    private def token(s: String) = {
      nextId += 1
      ExtBasicToken(nextId, s)
    }

    def intern(s: String): Token = tokMap.getOrElseUpdate(s, token(s))

    def tokenIterator = tokMap.values.iterator.asJava

    def maxTokenIndex = nextId

  }

}

object text extends App {

  import GraphFactory._
  import analyze._

  //  val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
  val taaableLabels = labelsFromQuadsUnique(new FileInputStream("ldif-taaable/taaable-food.nq"))
  val dbpediaLabels = labelsFromQuadsUnique(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))

  //  labelTokens(taaableLabels) foreach println
  //  labelTokens(dbpediaLabels)


  val tfidf1 = {
    val m = new TFIDF(ExtTokenizerSs(true))
    val v = taaableLabels.values map m.prepare
    m.train(new BasicStringWrapperIterator(v.iterator.asJava))
    m
  }

  val tfidf2 = {
    val m = new SoftTFIDF(ExtTokenizerSs(true), new MongeElkan, 0.10)
    val v = taaableLabels.values map m.prepare
    m.train(new BasicStringWrapperIterator(v.iterator.asJava))
    m
  }

  val tfidf3 = {
    val m = new SoftTFIDF(ExtTokenizerSs(true), new JaroWinkler, 0.9)
    val v = taaableLabels.values map m.prepare
    m.train(new BasicStringWrapperIterator(v.iterator.asJava))
    m
  }

  val tfidf4 = {
    val m = new SoftTFIDF(ExtTokenizerSs(true), new Jaro, 0.30)
    val v = taaableLabels.values map m.prepare
    m.train(new BasicStringWrapperIterator(v.iterator.asJava))
    m
  }

  val pairs = List(
    "blue rice" -> "green rice",
    "blues rice" -> "green rice",
    "blue beach" -> "blue beach",
    "blue beach" -> "blue peach",
    "blue beach" -> "blue reach"
  )

  pairs foreach {
    case (a, b) =>
      val s1 = tfidf1.score(a, b)
      val s2 = tfidf2.score(a, b)
      val s3 = tfidf3.score(a, b)
      val s4 = tfidf4.score(a, b)
      println(f"$a x $b = $s1%.4f $s2%.4f $s3%.4f $s4%.4f")
  }

  tfidf3.tokenIterator.asScala.toList map {
    t =>
      val tk = t.asInstanceOf[Token]
      val weight = tfidf3.getWeight(tk)
      (tk.getValue -> weight)
  } sortBy (_._2) foreach println
  //  println(tfidf3.getDocumentFrequency(new Token))

}