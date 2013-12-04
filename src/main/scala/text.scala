import java.io.{Reader, StringReader}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.en.PorterStemFilter
import org.apache.lucene.analysis.miscellaneous.LengthFilter
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.{FilteringTokenFilter, CharTokenizer}
import org.apache.lucene.analysis.{TokenStream, Analyzer}
import org.apache.lucene.util.Version

object analyze {

  /**
   * analyzer
   *
   * - tokenizer rules
   *   separator: ,.()'/
   *   no-separator: –-&!*+:
   *   alternative: split also at –-
   *
   * - length filter
   *   <= 1
   *
   * - numeric filter
   *
   * - stop word filter
   *   none, use weights instead
   *   maybe use a very basic stop word filter, since few training data is available
   *   internal training vs. externally trained weights
   *
   * - stemming: none
   *
   * - synonyms
   *
   */

  case class ExtCharTokenizer(matchVersion: Version, in: Reader) extends CharTokenizer(matchVersion, in) {
    protected def isTokenChar(c: Int): Boolean = {
      !(Character.isWhitespace(c) || c == ',' || c == '.' || c == '(' || c == ')' || c == '\'' || c == '/')
    }
  }

  case class ExtNumericFilter(v: Version, in: TokenStream) extends FilteringTokenFilter(v, in) {
    private val termAtt = addAttribute(classOf[CharTermAttribute])

    def accept: Boolean = {
      termAtt.buffer exists (c => !Character.isDigit(c))
    }
  }

  case class ExtAnalyzer(matchVersion: Version) extends Analyzer {
    protected def createComponents(fieldName: String, reader: Reader): TokenStreamComponents = {
      val source = ExtCharTokenizer(matchVersion, reader)
      // val source = new ClassicTokenizer(matchVersion, reader)

      val stemmer = new PorterStemFilter(source)

      val filter = new ExtNumericFilter(matchVersion,
        new LengthFilter(matchVersion,
          new LowerCaseFilter(matchVersion, source), 2, Integer.MAX_VALUE))

      new TokenStreamComponents(source, filter)
    }
  }

  val matchVersion = Version.LUCENE_46
  val analyzer = ExtAnalyzer(matchVersion)

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

}
