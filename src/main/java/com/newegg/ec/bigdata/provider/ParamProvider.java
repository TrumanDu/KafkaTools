package com.newegg.ec.bigdata.provider;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.core.MethodParameter;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProviderSupport;

/**
 * @author：Truman.P.Du
 * @createDate: 2018年12月7日 上午9:49:59
 * @version:1.0
 * @description: 命令参数提示供应者
 */
public class ParamProvider extends ValueProviderSupport {
	private final static String[] VALUES = new String[] { "generate-topics", "--zookeeper", "reassign-partitions",
			"--broker", "--topics-json-file" };

	@Override
	public List<CompletionProposal> complete(MethodParameter parameter, CompletionContext completionContext,
			String[] hints) {
		return Arrays.stream(VALUES).map(CompletionProposal::new).collect(Collectors.toList());
	}

}
