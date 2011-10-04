<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="html" indent="yes" encoding="UTF-8" omit-xml-declaration="yes" />
	<xsl:template match="/results">
	<html>
	<table border="1">
		<tr><th>Id</th><th>Title</th><th>Timestamp</th><th>Comments</th><th>Document Link</th></tr>
		<xsl:for-each select="document">
			<tr>
				<td><xsl:value-of select="field[@name = 'ID']/text()" /></td>
				<td><xsl:value-of select="field[@name = 'TITLE']/text()" /></td>
				<td><xsl:value-of select="field[@name = 'TIMESTAMP']/text()" /></td>
				<td><xsl:value-of select="field[@name = 'COMMENTS']/text()" /></td>
				<xsl:variable name="pointer" select="field[@name ='DOCUMENT']/text()" />
				<xsl:variable name="href">
					<xsl:text>/accumulo-sample/rest/Query/content?query=</xsl:text><xsl:copy-of select="$pointer"/><xsl:text>&amp;auths=all</xsl:text>
				</xsl:variable>
				<xsl:variable name="link">
					<xsl:element name="a">
						<xsl:attribute name="href"><xsl:copy-of select="$href" /></xsl:attribute>
						<xsl:attribute name="target">_blank</xsl:attribute>
						<xsl:text>View Document</xsl:text>
					</xsl:element>
				</xsl:variable>
				<td><xsl:copy-of select="$link"/></td>
			</tr>
		</xsl:for-each>
	</table>
	</html>
	</xsl:template>
</xsl:stylesheet>