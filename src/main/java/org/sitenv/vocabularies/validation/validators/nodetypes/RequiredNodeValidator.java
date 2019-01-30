package org.sitenv.vocabularies.validation.validators.nodetypes;

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPath;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.sitenv.vocabularies.configuration.ConfiguredValidationResultSeverityLevel;
import org.sitenv.vocabularies.configuration.ConfiguredValidator;
import org.sitenv.vocabularies.validation.dto.NodeValidationResult;
import org.sitenv.vocabularies.validation.dto.VocabularyValidationResult;
import org.sitenv.vocabularies.validation.dto.enums.VocabularyValidationResultLevel;
import org.sitenv.vocabularies.validation.utils.ConfiguredExpressionFilter;
import org.sitenv.vocabularies.validation.utils.XpathUtils;
import org.sitenv.vocabularies.validation.validators.NodeValidator;
import org.springframework.stereotype.Component;

import com.ximpleware.VTDNav;

@Component(value = "RequiredNodeValidator")
public class RequiredNodeValidator extends NodeValidator {
	
	private static Logger LOGGER = Logger.getLogger(RequiredNodeValidator.class); 
	private static final String AT_SIGN = "@";
	private static final short ONE = 1;

	@Override
	public List<VocabularyValidationResult> validateNode(ConfiguredValidator configuredValidator, XPath xpath,
			VTDNav vn, int nodeIndex, ConfiguredExpressionFilter filter, String xpathExpression) {	
		boolean hasNode = false;

    	LOGGER.debug("Entered RequiredNodeValidator.validateNode() with xpathExpression = " + xpathExpression);
    	
        try {
        	String nodeName = configuredValidator.getRequiredNodeName();
//        	LOGGER.debug("nodeName: " + nodeName);

        	if (StringUtils.isBlank(nodeName)) {
        		LOGGER.error("nodeName " + nodeName + " is null or blank"); 
        	} else if (nodeName.startsWith(AT_SIGN)) { // It's an attribute such as @unit
        		String attrName = nodeName.substring(ONE);
        		hasNode = vn.hasAttr(attrName);
//        		LOGGER.debug("Found Attribute: " + attrName);
        	} else { // It's an element
        		hasNode = vn.matchElement(nodeName);
//        		LOGGER.debug("Found Element: " + nodeName);        		
        	}
        	
//        	LOGGER.debug("hasNode = " + hasNode);
        } catch (Exception e) {
			throw new RuntimeException("ERROR parsing document with given XPath expression: " + e.getMessage());
        }

		NodeValidationResult nodeValidationResult = new NodeValidationResult();
        nodeValidationResult.setErrorOffset(vn.getTokenOffset(vn.getCurrentIndex()));
        nodeValidationResult.setValidatedDocumentXpathExpression(xpathExpression);
        nodeValidationResult.setRequestedNode(configuredValidator.getRequiredNodeName());
        nodeValidationResult.setConfiguredValidationMessage(configuredValidator.getValidationMessage());
        nodeValidationResult.setRuleID(configuredValidator.getId());
        
        if(hasNode) {
        	nodeValidationResult.setValid(true);
        }
        
		return buildVocabularyValidationResults(nodeValidationResult, configuredValidator.getConfiguredValidationResultSeverityLevel(), filter, vn);    
	}

	@Override
	protected List<VocabularyValidationResult> buildVocabularyValidationResults(
			NodeValidationResult nodeValidationResult,
			ConfiguredValidationResultSeverityLevel configuredValidationResultSeverityLevel, ConfiguredExpressionFilter filter, VTDNav nav) {
        List<VocabularyValidationResult> vocabularyValidationResults = new ArrayList<>();
		
		if (!nodeValidationResult.isValid()) {
			VocabularyValidationResult vocabularyValidationResult = new VocabularyValidationResult();
			
			nodeValidationResult.setValidatedDocumentXpathExpression(XpathUtils.getVTDXPath(nav));
//			LOGGER.debug("nodeValidationResult.getValidatedDocumentXpathExpression() = " + nodeValidationResult.getValidatedDocumentXpathExpression());
			
			vocabularyValidationResult.setNodeValidationResult(nodeValidationResult);
			vocabularyValidationResult.setVocabularyValidationResultLevel(VocabularyValidationResultLevel
					.valueOf(configuredValidationResultSeverityLevel.getCodeSeverityLevel()));
		
//			LOGGER.debug("vocabularyValidationResult.getVocabularyValidationResultLevel() = " + vocabularyValidationResult.getVocabularyValidationResultLevel());

			String finalValidationMessage = "The node '" + nodeValidationResult.getRequestedNode()
					+ "' does not exist at the expected path "
					+ nodeValidationResult.getValidatedDocumentXpathExpression()
					+ " but is required as per the specification: "
					+ nodeValidationResult.getConfiguredValidationMessage();
                vocabularyValidationResult.setMessage(finalValidationMessage);
                vocabularyValidationResults.add(vocabularyValidationResult);
        }
		
		return vocabularyValidationResults;
	}

}
