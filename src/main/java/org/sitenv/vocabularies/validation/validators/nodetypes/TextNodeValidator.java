package org.sitenv.vocabularies.validation.validators.nodetypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.xpath.XPath;

import org.apache.log4j.Logger;
import org.sitenv.vocabularies.configuration.ConfiguredValidationResultSeverityLevel;
import org.sitenv.vocabularies.configuration.ConfiguredValidator;
import org.sitenv.vocabularies.validation.dao.ValueSetDAO;
import org.sitenv.vocabularies.validation.dto.NodeValidationResult;
import org.sitenv.vocabularies.validation.dto.VocabularyValidationResult;
import org.sitenv.vocabularies.validation.dto.enums.VocabularyValidationResultLevel;
import org.sitenv.vocabularies.validation.utils.ConfiguredExpressionFilter;
import org.sitenv.vocabularies.validation.utils.XpathUtils;
import org.sitenv.vocabularies.validation.validators.NodeValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ximpleware.VTDNav;

@Component(value = "TextNodeValidator")
public class TextNodeValidator extends NodeValidator {
	private static final Logger logger = Logger.getLogger(TextNodeValidator.class);
//	private VsacValuesSetRepository vsacValuesSetRepository;

    @Autowired
    ValueSetDAO vsacValuesSetRepository;
	
//	@Autowired
//	public TextNodeValidator(VsacValuesSetRepository vsacValuesSetRepository) {
//		this.vsacValuesSetRepository = vsacValuesSetRepository;
//	}

	@Override
	public List<VocabularyValidationResult> validateNode(ConfiguredValidator configuredValidator, XPath xpath,
			VTDNav vn, int nodeIndex, ConfiguredExpressionFilter filter, String xpathExpression) {
        String nodeText;
        try {
			nodeText = vn.toString(vn.getText()).toUpperCase();
        } catch (Exception e) {
			throw new RuntimeException("ERROR getting node values " + e.getMessage());
        }

        List<String> allowedConfiguredCodeSystemOids = new ArrayList<>(Arrays.asList(configuredValidator.getAllowedValuesetOids().split(",")));

		NodeValidationResult nodeValidationResult = new NodeValidationResult();
        nodeValidationResult.setErrorOffset(vn.getTokenOffset(vn.getCurrentIndex()));
        nodeValidationResult.setValidatedDocumentXpathExpression(xpathExpression);
        nodeValidationResult.setRequestedText(nodeText);
        nodeValidationResult.setConfiguredAllowableValuesetOidsForNode(configuredValidator.getAllowedValuesetOids());
        nodeValidationResult.setRuleID(configuredValidator.getId());
        
		if(vsacValuesSetRepository.valuesetOidsExists(allowedConfiguredCodeSystemOids)){
            nodeValidationResult.setNodeValuesetsFound(true);
			if (vsacValuesSetRepository.codeExistsInValueset(nodeText, allowedConfiguredCodeSystemOids)) {
                nodeValidationResult.setValid(true);
			}
		}
		return buildVocabularyValidationResults(nodeValidationResult, configuredValidator.getConfiguredValidationResultSeverityLevel(), filter, vn);
	}
	
	@Override
	protected List<VocabularyValidationResult> buildVocabularyValidationResults(NodeValidationResult nodeValidationResult, ConfiguredValidationResultSeverityLevel configuredNodeAttributeSeverityLevel, ConfiguredExpressionFilter filter, VTDNav nav) {
        List<VocabularyValidationResult> vocabularyValidationResults = new ArrayList<>();
        if(!nodeValidationResult.isValid()) {
            if (nodeValidationResult.isNodeValuesetsFound()) {
                VocabularyValidationResult vocabularyValidationResult = new VocabularyValidationResult();
				nodeValidationResult.setValidatedDocumentXpathExpression(XpathUtils.getVTDXPath(nav));
                vocabularyValidationResult.setNodeValidationResult(nodeValidationResult);
                vocabularyValidationResult.setVocabularyValidationResultLevel(VocabularyValidationResultLevel.valueOf(configuredNodeAttributeSeverityLevel.getCodeSeverityLevel()));
                String validationMessage = nodeValidationResult.getRequestedText() + "' does not exist in the value set (" + nodeValidationResult.getConfiguredAllowableValuesetOidsForNode() + ")";
                vocabularyValidationResult.setMessage(validationMessage);
                vocabularyValidationResults.add(vocabularyValidationResult);
            }else{
				nodeValidationResult.setValidatedDocumentXpathExpression(XpathUtils.getVTDXPath(nav));
                vocabularyValidationResults.add(valuesetNotLoadedResult(nodeValidationResult));
            }
        }
		return vocabularyValidationResults;
	}
}
