# Generated from influxdb.g4 by ANTLR 4.7
# encoding: utf-8
from antlr4 import *
from io import StringIO
from typing.io import TextIO
import sys

def serializedATN():
    with StringIO() as buf:
        buf.write("\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\t")
        buf.write("^\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b")
        buf.write("\t\b\4\t\t\t\4\n\t\n\4\13\t\13\3\2\6\2\30\n\2\r\2\16\2")
        buf.write("\31\3\3\3\3\3\3\3\3\3\3\5\3!\n\3\3\3\3\3\5\3%\n\3\3\4")
        buf.write("\3\4\3\5\3\5\3\5\3\5\5\5-\n\5\3\5\3\5\3\5\7\5\62\n\5\f")
        buf.write("\5\16\5\65\13\5\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3")
        buf.write("\7\7\7A\n\7\f\7\16\7D\13\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b")
        buf.write("\3\b\3\b\3\b\3\b\3\b\5\bR\n\b\3\t\3\t\3\t\3\n\3\n\3\13")
        buf.write("\5\13Z\n\13\3\13\3\13\3\13\2\4\b\f\f\2\4\6\b\n\f\16\20")
        buf.write("\22\24\2\2\2]\2\27\3\2\2\2\4\33\3\2\2\2\6&\3\2\2\2\b,")
        buf.write("\3\2\2\2\n\66\3\2\2\2\f:\3\2\2\2\16Q\3\2\2\2\20S\3\2\2")
        buf.write("\2\22V\3\2\2\2\24Y\3\2\2\2\26\30\5\4\3\2\27\26\3\2\2\2")
        buf.write("\30\31\3\2\2\2\31\27\3\2\2\2\31\32\3\2\2\2\32\3\3\2\2")
        buf.write("\2\33\34\5\6\4\2\34\35\5\b\5\2\35\36\5\22\n\2\36 \5\f")
        buf.write("\7\2\37!\5\20\t\2 \37\3\2\2\2 !\3\2\2\2!$\3\2\2\2\"%\5")
        buf.write("\24\13\2#%\7\2\2\3$\"\3\2\2\2$#\3\2\2\2%\5\3\2\2\2&\'")
        buf.write("\7\b\2\2\'\7\3\2\2\2()\b\5\1\2)*\7\3\2\2*-\5\n\6\2+-\3")
        buf.write("\2\2\2,(\3\2\2\2,+\3\2\2\2-\63\3\2\2\2./\f\5\2\2/\60\7")
        buf.write("\3\2\2\60\62\5\n\6\2\61.\3\2\2\2\62\65\3\2\2\2\63\61\3")
        buf.write("\2\2\2\63\64\3\2\2\2\64\t\3\2\2\2\65\63\3\2\2\2\66\67")
        buf.write("\7\b\2\2\678\7\4\2\289\7\b\2\29\13\3\2\2\2:;\b\7\1\2;")
        buf.write("<\5\16\b\2<B\3\2\2\2=>\f\4\2\2>?\7\3\2\2?A\5\16\b\2@=")
        buf.write("\3\2\2\2AD\3\2\2\2B@\3\2\2\2BC\3\2\2\2C\r\3\2\2\2DB\3")
        buf.write("\2\2\2EF\7\b\2\2FG\7\4\2\2GR\7\b\2\2HI\7\b\2\2IJ\7\4\2")
        buf.write("\2JR\7\b\2\2KL\7\b\2\2LM\7\4\2\2MR\7\b\2\2NO\7\b\2\2O")
        buf.write("P\7\4\2\2PR\7\t\2\2QE\3\2\2\2QH\3\2\2\2QK\3\2\2\2QN\3")
        buf.write("\2\2\2R\17\3\2\2\2ST\5\22\n\2TU\7\b\2\2U\21\3\2\2\2VW")
        buf.write("\7\5\2\2W\23\3\2\2\2XZ\7\6\2\2YX\3\2\2\2YZ\3\2\2\2Z[\3")
        buf.write("\2\2\2[\\\7\7\2\2\\\25\3\2\2\2\n\31 $,\63BQY")
        return buf.getvalue()


class influxdbParser ( Parser ):

    grammarFileName = "influxdb.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "','", "'='", "' '", "'\r'", "'\n'" ]

    symbolicNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                      "<INVALID>", "<INVALID>", "INFLUXWORD", "INFLUXSTRING" ]

    RULE_lines = 0
    RULE_line = 1
    RULE_metric = 2
    RULE_tags = 3
    RULE_ttype = 4
    RULE_values = 5
    RULE_vtype = 6
    RULE_timestamp = 7
    RULE_space = 8
    RULE_newline = 9

    ruleNames =  [ "lines", "line", "metric", "tags", "ttype", "values", 
                   "vtype", "timestamp", "space", "newline" ]

    EOF = Token.EOF
    T__0=1
    T__1=2
    T__2=3
    T__3=4
    T__4=5
    INFLUXWORD=6
    INFLUXSTRING=7

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.7")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None



    class LinesContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def line(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(influxdbParser.LineContext)
            else:
                return self.getTypedRuleContext(influxdbParser.LineContext,i)


        def getRuleIndex(self):
            return influxdbParser.RULE_lines

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLines" ):
                listener.enterLines(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLines" ):
                listener.exitLines(self)




    def lines(self):

        localctx = influxdbParser.LinesContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_lines)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 21 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 20
                self.line()
                self.state = 23 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (_la==influxdbParser.INFLUXWORD):
                    break

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class LineContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def metric(self):
            return self.getTypedRuleContext(influxdbParser.MetricContext,0)


        def tags(self):
            return self.getTypedRuleContext(influxdbParser.TagsContext,0)


        def space(self):
            return self.getTypedRuleContext(influxdbParser.SpaceContext,0)


        def values(self):
            return self.getTypedRuleContext(influxdbParser.ValuesContext,0)


        def newline(self):
            return self.getTypedRuleContext(influxdbParser.NewlineContext,0)


        def EOF(self):
            return self.getToken(influxdbParser.EOF, 0)

        def timestamp(self):
            return self.getTypedRuleContext(influxdbParser.TimestampContext,0)


        def getRuleIndex(self):
            return influxdbParser.RULE_line

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLine" ):
                listener.enterLine(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLine" ):
                listener.exitLine(self)




    def line(self):

        localctx = influxdbParser.LineContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_line)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 25
            self.metric()
            self.state = 26
            self.tags(0)
            self.state = 27
            self.space()
            self.state = 28
            self.values(0)
            self.state = 30
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==influxdbParser.T__2:
                self.state = 29
                self.timestamp()


            self.state = 34
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [influxdbParser.T__3, influxdbParser.T__4]:
                self.state = 32
                self.newline()
                pass
            elif token in [influxdbParser.EOF]:
                self.state = 33
                self.match(influxdbParser.EOF)
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MetricContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INFLUXWORD(self):
            return self.getToken(influxdbParser.INFLUXWORD, 0)

        def getRuleIndex(self):
            return influxdbParser.RULE_metric

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMetric" ):
                listener.enterMetric(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMetric" ):
                listener.exitMetric(self)




    def metric(self):

        localctx = influxdbParser.MetricContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_metric)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 36
            self.match(influxdbParser.INFLUXWORD)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TagsContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ttype(self):
            return self.getTypedRuleContext(influxdbParser.TtypeContext,0)


        def tags(self):
            return self.getTypedRuleContext(influxdbParser.TagsContext,0)


        def getRuleIndex(self):
            return influxdbParser.RULE_tags

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTags" ):
                listener.enterTags(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTags" ):
                listener.exitTags(self)



    def tags(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = influxdbParser.TagsContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 6
        self.enterRecursionRule(localctx, 6, self.RULE_tags, _p)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 42
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,3,self._ctx)
            if la_ == 1:
                self.state = 39
                self.match(influxdbParser.T__0)
                self.state = 40
                self.ttype()
                pass

            elif la_ == 2:
                pass


            self._ctx.stop = self._input.LT(-1)
            self.state = 49
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,4,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    localctx = influxdbParser.TagsContext(self, _parentctx, _parentState)
                    self.pushNewRecursionContext(localctx, _startState, self.RULE_tags)
                    self.state = 44
                    if not self.precpred(self._ctx, 3):
                        from antlr4.error.Errors import FailedPredicateException
                        raise FailedPredicateException(self, "self.precpred(self._ctx, 3)")
                    self.state = 45
                    self.match(influxdbParser.T__0)
                    self.state = 46
                    self.ttype() 
                self.state = 51
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,4,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class TtypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INFLUXWORD(self, i:int=None):
            if i is None:
                return self.getTokens(influxdbParser.INFLUXWORD)
            else:
                return self.getToken(influxdbParser.INFLUXWORD, i)

        def getRuleIndex(self):
            return influxdbParser.RULE_ttype

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTtype" ):
                listener.enterTtype(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTtype" ):
                listener.exitTtype(self)




    def ttype(self):

        localctx = influxdbParser.TtypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_ttype)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 52
            self.match(influxdbParser.INFLUXWORD)
            self.state = 53
            self.match(influxdbParser.T__1)
            self.state = 54
            self.match(influxdbParser.INFLUXWORD)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ValuesContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def vtype(self):
            return self.getTypedRuleContext(influxdbParser.VtypeContext,0)


        def values(self):
            return self.getTypedRuleContext(influxdbParser.ValuesContext,0)


        def getRuleIndex(self):
            return influxdbParser.RULE_values

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterValues" ):
                listener.enterValues(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitValues" ):
                listener.exitValues(self)



    def values(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = influxdbParser.ValuesContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 10
        self.enterRecursionRule(localctx, 10, self.RULE_values, _p)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 57
            self.vtype()
            self._ctx.stop = self._input.LT(-1)
            self.state = 64
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,5,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    localctx = influxdbParser.ValuesContext(self, _parentctx, _parentState)
                    self.pushNewRecursionContext(localctx, _startState, self.RULE_values)
                    self.state = 59
                    if not self.precpred(self._ctx, 2):
                        from antlr4.error.Errors import FailedPredicateException
                        raise FailedPredicateException(self, "self.precpred(self._ctx, 2)")
                    self.state = 60
                    self.match(influxdbParser.T__0)
                    self.state = 61
                    self.vtype() 
                self.state = 66
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,5,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class VtypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INFLUXWORD(self, i:int=None):
            if i is None:
                return self.getTokens(influxdbParser.INFLUXWORD)
            else:
                return self.getToken(influxdbParser.INFLUXWORD, i)

        def INFLUXSTRING(self):
            return self.getToken(influxdbParser.INFLUXSTRING, 0)

        def getRuleIndex(self):
            return influxdbParser.RULE_vtype

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterVtype" ):
                listener.enterVtype(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitVtype" ):
                listener.exitVtype(self)




    def vtype(self):

        localctx = influxdbParser.VtypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 12, self.RULE_vtype)
        try:
            self.state = 79
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,6,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 67
                self.match(influxdbParser.INFLUXWORD)
                self.state = 68
                self.match(influxdbParser.T__1)
                self.state = 69
                self.match(influxdbParser.INFLUXWORD)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 70
                self.match(influxdbParser.INFLUXWORD)
                self.state = 71
                self.match(influxdbParser.T__1)
                self.state = 72
                self.match(influxdbParser.INFLUXWORD)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 73
                self.match(influxdbParser.INFLUXWORD)
                self.state = 74
                self.match(influxdbParser.T__1)
                self.state = 75
                self.match(influxdbParser.INFLUXWORD)
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 76
                self.match(influxdbParser.INFLUXWORD)
                self.state = 77
                self.match(influxdbParser.T__1)
                self.state = 78
                self.match(influxdbParser.INFLUXSTRING)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TimestampContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def space(self):
            return self.getTypedRuleContext(influxdbParser.SpaceContext,0)


        def INFLUXWORD(self):
            return self.getToken(influxdbParser.INFLUXWORD, 0)

        def getRuleIndex(self):
            return influxdbParser.RULE_timestamp

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTimestamp" ):
                listener.enterTimestamp(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTimestamp" ):
                listener.exitTimestamp(self)




    def timestamp(self):

        localctx = influxdbParser.TimestampContext(self, self._ctx, self.state)
        self.enterRule(localctx, 14, self.RULE_timestamp)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 81
            self.space()
            self.state = 82
            self.match(influxdbParser.INFLUXWORD)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SpaceContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return influxdbParser.RULE_space

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSpace" ):
                listener.enterSpace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSpace" ):
                listener.exitSpace(self)




    def space(self):

        localctx = influxdbParser.SpaceContext(self, self._ctx, self.state)
        self.enterRule(localctx, 16, self.RULE_space)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 84
            self.match(influxdbParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NewlineContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return influxdbParser.RULE_newline

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNewline" ):
                listener.enterNewline(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNewline" ):
                listener.exitNewline(self)




    def newline(self):

        localctx = influxdbParser.NewlineContext(self, self._ctx, self.state)
        self.enterRule(localctx, 18, self.RULE_newline)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 87
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==influxdbParser.T__3:
                self.state = 86
                self.match(influxdbParser.T__3)


            self.state = 89
            self.match(influxdbParser.T__4)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx



    def sempred(self, localctx:RuleContext, ruleIndex:int, predIndex:int):
        if self._predicates == None:
            self._predicates = dict()
        self._predicates[3] = self.tags_sempred
        self._predicates[5] = self.values_sempred
        pred = self._predicates.get(ruleIndex, None)
        if pred is None:
            raise Exception("No predicate with index:" + str(ruleIndex))
        else:
            return pred(localctx, predIndex)

    def tags_sempred(self, localctx:TagsContext, predIndex:int):
            if predIndex == 0:
                return self.precpred(self._ctx, 3)
         

    def values_sempred(self, localctx:ValuesContext, predIndex:int):
            if predIndex == 1:
                return self.precpred(self._ctx, 2)
         




