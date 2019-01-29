package org.sitenv.vocabularies.validation.utils;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.ximpleware.VTDNav;

/**
 * Created by Brian on 10/20/2015.
 */
public class XpathUtils {
	private static Logger LOGGER = Logger.getLogger(XpathUtils.class); 
	
	public static String buildXpathFromNode(Node node) {
		return getXPath(node, "");
	}

	private static String getXPath(Node node, String xpath) {
		String elementName = "";
		if (node instanceof Element) {
			elementName = node.getNodeName();
			int prev_siblings = 1;
			Node prev_sibling = node.getPreviousSibling();
			while (null != prev_sibling) {
				if (prev_sibling.getNodeType() == node.getNodeType()) {
					if (prev_sibling.getNodeName().equalsIgnoreCase(node.getNodeName())) {
						prev_siblings++;
					}
				}
				prev_sibling = prev_sibling.getPreviousSibling();
			}
			elementName = elementName.concat("[" + prev_siblings + "]");
		}
		Node parent = node.getParentNode();
		if (parent == null) {
			return xpath;
		}
		return getXPath(parent, "/" + elementName + xpath);
	}

	// Build XPath from the current leaf node all the way up to the XML root element
	public static String getVTDXPath(VTDNav vn) {

		// Save off current navigator state
		vn.push();
		String xp = ""; 

		try {
//			LOGGER.debug("Before 1st while, node = " + vn.toNormalizedString(vn.getCurrentIndex()) + ", tokenType = " 
//				+ vn.getTokenType(vn.getCurrentIndex()));
			 
			// Move to a parent that is a node.  If the current node is an attribute, find the element that contains this attribute
			while (vn.getTokenType(vn.getCurrentIndex()) != 0 && vn.getTokenType(vn.getCurrentIndex()) != VTDNav.TOKEN_DOCUMENT) {
				vn.toElement(VTDNav.PARENT);
			}
			
//			LOGGER.debug("After 1st while, node = " + vn.toNormalizedString(vn.getCurrentIndex()) + ", tokenType = " 
//				+ vn.getTokenType(vn.getCurrentIndex()));

			String srcElement = null;

			while (vn.getTokenType(vn.getCurrentIndex()) != VTDNav.TOKEN_DOCUMENT) {

				srcElement = vn.toNormalizedString(vn.getCurrentIndex());
				
//				LOGGER.debug("node = " + vn.toNormalizedString(vn.getCurrentIndex()) + ", tokenType = " 
//					+ vn.getTokenType(vn.getCurrentIndex()));
				
				// Find depth, i.e., the index # of the srcElement.  For example: d = 3 means there are 2 sibling elements of the same name before this element
				int d = 1;
				boolean e = vn.toElement(VTDNav.PREV_SIBLING); 

				while (e) {
					if (vn.matchElement(srcElement)) { // We must match the element name, as PREV_SIBLING will pull in elements of other names at the same level
						d++;
					}
					e = vn.toElement(VTDNav.PREV_SIBLING);
				}
			
				xp = "/" + srcElement + "[" + d + "]" + xp;
				
//				LOGGER.debug("xp = " + xp);

				vn.toElement(VTDNav.PARENT); // Move to the parent element
			}
		} catch (Exception e) {
			return "Can not determine xpath address. Error: " + e.getMessage();
		}

		// Restore the navigator
		vn.pop();

		return xp;
	}
}
