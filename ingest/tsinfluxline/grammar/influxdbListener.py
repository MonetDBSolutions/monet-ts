# Generated from influxdb.g4 by ANTLR 4.7
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .influxdbParser import influxdbParser
else:
    from influxdbParser import influxdbParser

# This class defines a complete listener for a parse tree produced by influxdbParser.
class influxdbListener(ParseTreeListener):

    # Enter a parse tree produced by influxdbParser#lines.
    def enterLines(self, ctx:influxdbParser.LinesContext):
        pass

    # Exit a parse tree produced by influxdbParser#lines.
    def exitLines(self, ctx:influxdbParser.LinesContext):
        pass


    # Enter a parse tree produced by influxdbParser#line.
    def enterLine(self, ctx:influxdbParser.LineContext):
        pass

    # Exit a parse tree produced by influxdbParser#line.
    def exitLine(self, ctx:influxdbParser.LineContext):
        pass


    # Enter a parse tree produced by influxdbParser#metric.
    def enterMetric(self, ctx:influxdbParser.MetricContext):
        pass

    # Exit a parse tree produced by influxdbParser#metric.
    def exitMetric(self, ctx:influxdbParser.MetricContext):
        pass


    # Enter a parse tree produced by influxdbParser#tags.
    def enterTags(self, ctx:influxdbParser.TagsContext):
        pass

    # Exit a parse tree produced by influxdbParser#tags.
    def exitTags(self, ctx:influxdbParser.TagsContext):
        pass


    # Enter a parse tree produced by influxdbParser#ttype.
    def enterTtype(self, ctx:influxdbParser.TtypeContext):
        pass

    # Exit a parse tree produced by influxdbParser#ttype.
    def exitTtype(self, ctx:influxdbParser.TtypeContext):
        pass


    # Enter a parse tree produced by influxdbParser#values.
    def enterValues(self, ctx:influxdbParser.ValuesContext):
        pass

    # Exit a parse tree produced by influxdbParser#values.
    def exitValues(self, ctx:influxdbParser.ValuesContext):
        pass


    # Enter a parse tree produced by influxdbParser#vtype.
    def enterVtype(self, ctx:influxdbParser.VtypeContext):
        pass

    # Exit a parse tree produced by influxdbParser#vtype.
    def exitVtype(self, ctx:influxdbParser.VtypeContext):
        pass


    # Enter a parse tree produced by influxdbParser#timestamp.
    def enterTimestamp(self, ctx:influxdbParser.TimestampContext):
        pass

    # Exit a parse tree produced by influxdbParser#timestamp.
    def exitTimestamp(self, ctx:influxdbParser.TimestampContext):
        pass


    # Enter a parse tree produced by influxdbParser#space.
    def enterSpace(self, ctx:influxdbParser.SpaceContext):
        pass

    # Exit a parse tree produced by influxdbParser#space.
    def exitSpace(self, ctx:influxdbParser.SpaceContext):
        pass


    # Enter a parse tree produced by influxdbParser#newline.
    def enterNewline(self, ctx:influxdbParser.NewlineContext):
        pass

    # Exit a parse tree produced by influxdbParser#newline.
    def exitNewline(self, ctx:influxdbParser.NewlineContext):
        pass


