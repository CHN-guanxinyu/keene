package com.jd.ad.anti.cps.filter

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.Try

class Space(val expr: String, val variables: Map[String, Any]) extends Serializable {
  val e = new LogicalExpression(variables).parse(expr)
  def contains(record: Map[String, Any]): Boolean = e.eval(record)
}

// BNF
// <b-expression>  ::= <b-term> [or <b-term>]*
// <b-term>        ::= <not-factor> [and <not-factor>]*
// <not-factor>    ::= [not] <b-factor>
// <b-factor>      ::= <b-literal> | <num-comp-expr> | <str-comp-expr> | (<b-expression>)
// <b-literal>     ::= true | false
// <num-comp-expr> ::= <n-factor> "<="|">="|">"|"<"|"=="|"!=" <n-factor>
// <str-comp-expr> ::= <s-factor> "=="|"!="|"ct" <s-factor>
// <n-factor>      ::= <decimalNumber> | <floatingPointNumber> | <ident>
// <s-factor>      ::= <stringLiteral> | <ident>
class LogicalExpression(val variables: Map[String, Any]) extends JavaTokenParsers {
  def parse(expr: String) = this.parseAll(boolExpr, expr).get
  override def ident: Parser[String] = 
    //"""[a-zA-Z_]\w*""".r
    """[a-zA-Z_][\w\.]*""".r
  private def boolExpr: Parser[BooleanExpr] =
    boolTerm ~ rep("or" ~ boolTerm) ^^ {
      case f1 ~ List() => f1
      case f1 ~ fs => fs.map(_._2).foldLeft(f1)(AndOrExpr("or", _, _))}
  private def boolTerm: Parser[BooleanExpr] =
    (boolNotFactor ~ rep("and" ~ boolNotFactor)) ^^ {
      case f1 ~ List() => f1
      case f1 ~ fs => fs.map(_._2).foldLeft(f1)(AndOrExpr("and", _, _)) }
  private def boolNotFactor: Parser[BooleanExpr] =
    opt("not") ~ boolFactor ^^ (x => x match {
      case Some(v) ~ f => NotExpr(f)
      case None ~ f => f })
  private def boolFactor: Parser[BooleanExpr] =
    boolLiteral | numCmpExpr | strCmpExpr | numInExpr | strInExpr | variableExpr |
      ("(" ~ boolExpr ~ ")" ^^ { case "(" ~ exp ~ ")" => exp })
  private def boolLiteral: Parser[BooleanExpr] =
    ("true" | "false") ^^ (BooleanLiteral(_))

  private def numCmpExpr: Parser[BooleanExpr] =
    (numFactor ~ ("lt"|"gt"|"le"|"ge"|"eq"|"ne") ~ numFactor) ^^ {
      case left ~ op ~ right => NumCmpExpr(op, left, right) }
  private def numFactor: Parser[NumericFactor] =
    (decimalNumber | floatingPointNumber) ^^
      (n => Number(n.toDouble)) | ident ^^ (n => NumericVarable(n.toString)) |
      ("(" ~> ident ~ "minus" ~ ident <~ ")" ^^ 
        {case a ~ _ ~ b => NumMinusExpr(a, b)}) |
      ("(" ~> ident ~ "div" ~ ident <~ ")" ^^ 
        {case a ~ _ ~ b => NumDivExpr(a, b)})

  private def intSet: Parser[Set[String]] =
    "[" ~> wholeNumber ~ rep("," ~ wholeNumber) <~ "]" ^^ {
      case f1 ~ fs => Set(f1) ++ fs.map(_._2).toSet }
  private def numInExpr: Parser[BooleanExpr] =
    (ident ~ ("not").? ~ "in" ~ intSet) ^^ {
      case name ~ hasNot ~ _ ~ set => NumInExpr(name, hasNot.isDefined, set) }
 
  private def strSet: Parser[Set[StringFactor]] =
    "[" ~> strFactor ~ rep("," ~ strFactor) <~ "]" ^^ {
      case f1 ~ fs => Set(f1) ++ fs.map(_._2).toSet }
  private def strInExpr: Parser[BooleanExpr] =
    (ident ~ ("not").? ~ "in" ~ strSet) ^^ {
      case name ~ hasNot ~ _ ~ set => StrInExpr(name, hasNot.isDefined, set) }

  private def strCmpExpr: Parser[BooleanExpr] =
    (strFactor ~ ("=="|"!="|"ct") ~ strFactor) ^^ {
      case left ~ op ~ right => StrCmpExpr(op, left, right) }
  private def strFactor: Parser[StringFactor] = stringLiteral ^^
    (n => StringLiteral(n)) | ident ^^ (n => StringVarable(n.toString))
  
  private def argsList: Parser[List[String]] =
    "(" ~> ident ~ rep("," ~ ident) <~ ")" ^^ {
      case f1 ~ fs => List(f1) ++ fs.map(_._2).toList }
  private def variableExpr: Parser[BooleanExpr] =
    (ident ~ "called" ~ argsList) ^^ {
      case name ~ _ ~ args => {
        val parts = name.split("\\.")
        VariableExpr(variables.getOrElse(parts(0), null), parts(1), args)
      }
  }
}

trait BooleanExpr extends Serializable {
  def eval(variable: Map[String, Any]): Boolean
}

case class AndOrExpr(op: String, left: BooleanExpr, right:BooleanExpr) extends BooleanExpr {
  def eval(variable: Map[String, Any]): Boolean = {
    op match {
      case "and" => left.eval(variable) && right.eval(variable)
      case "or" => left.eval(variable) || right.eval(variable)
    }
  }
}

case class NotExpr(expr: BooleanExpr) extends BooleanExpr {
  def eval(variable: Map[String, Any]): Boolean = !expr.eval(variable)
}

case class BooleanLiteral(literal: String) extends BooleanExpr {
  val value = literal.toBoolean
  def eval(variable: Map[String, Any]): Boolean = value
}


trait NumericFactor extends Serializable {
  def eval(variable: Map[String, Any]): Double
}

case class Number(value: Double) extends NumericFactor {
  def eval(variable: Map[String, Any]): Double = value
}

case class NumericVarable(name: String) extends NumericFactor {
  def eval(variable: Map[String, Any]): Double = {
    variable.getOrElse(name, "0.0").toString.toDouble
  }
}

case class NumMinusExpr(a: String, b: String) extends NumericFactor {
  def eval(variable: Map[String, Any]): Double = {
    val firstVal = variable.getOrElse(a, "0.0").toString
    val secondVal = variable.getOrElse(b, "0.0").toString
    if (!Try(firstVal.toDouble).isSuccess 
      || !Try(secondVal.toDouble).isSuccess) {
      0f
    } else {
      firstVal.toDouble - secondVal.toDouble
    }
  }
}

case class NumDivExpr(a: String, b: String) extends NumericFactor {
  def eval(variable: Map[String, Any]): Double = {
    val divisor = variable.getOrElse(b, "0.0").toString
    if (!Try(divisor.toDouble).isSuccess || divisor.toDouble == 0) {
      0D
    } else {
      variable.getOrElse(a, "0.0").toString.toDouble / divisor.toDouble
    }
  }
}

case class NumCmpExpr(op: String, left: NumericFactor, right:NumericFactor) extends BooleanExpr {
  def eval(variable: Map[String, Any]): Boolean = {
    val leftVal = left.eval(variable)
    val rightVal = right.eval(variable)
    op match {
      case "gt" => leftVal > rightVal
      case "lt" => leftVal < rightVal
      case "ge" => leftVal >= rightVal
      case "le" => leftVal <= rightVal
      case "eq" => leftVal == rightVal
      case "ne" => leftVal != rightVal
    }
  }
}

case class NumInExpr(name: String, hasNot: Boolean, set: Set[String]) extends BooleanExpr {
  def eval(variable: Map[String, Any]): Boolean = {
    if (variable(name) != null) {
      val value = variable(name).toString
      return if (!hasNot) set.contains(value) else !set.contains(value)
    } else {
      println("no key " + name + " in recoard")
      return false
    }
  }
}

case class StrInExpr(name: String, hasNot: Boolean, set: Set[StringFactor]) extends BooleanExpr {
  def eval(variable: Map[String, Any]): Boolean = {
    val value = variable(name).toString
    val valueSet = set.map{x => x.eval(variable)}
    return if (!hasNot) valueSet.contains(value) else !valueSet.contains(value)
  }
}

trait StringFactor extends Serializable {
  def eval(variable: Map[String, Any]): String
}

case class StringLiteral(value: String) extends StringFactor {
  var literal = value.drop(1).dropRight(1)
  def eval(variable: Map[String, Any]): String = literal
}

case class StringVarable(name: String) extends StringFactor {
  def eval(variable: Map[String, Any]): String = {
    val v = variable.getOrElse(name, "")
    if (v != null)
      v.toString
    else
      ""
  }
}

case class StrCmpExpr(op: String, left: StringFactor, right: StringFactor) extends BooleanExpr {
  def eval(variable: Map[String, Any]): Boolean = {
    val leftVal = left.eval(variable)
    val rightVal = right.eval(variable)
    op match {
      case "==" => leftVal == rightVal
      case "!=" => leftVal != rightVal
      case "ct" => leftVal.indexOf(rightVal) != -1
    }
  }
}

case class VariableExpr(obj: Any,
  methodName: String, 
  args: List[String]) extends BooleanExpr {
  def eval(record: Map[String, Any]): Boolean = {
    var returnValue = false
    if (obj != null) {
      val values = args.map {k => 
      val v = record.getOrElse(k, "")
      if (v != null)
        v.toString
      else
        ""
      }
      val method = obj.getClass.getMethod(methodName, classOf[List[String]])
      returnValue = method.invoke(obj, values).toString.toBoolean
    }
    returnValue
  }
}
