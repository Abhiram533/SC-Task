package com.abhi.source.Configuration;

import org.springframework.batch.item.ItemProcessor;

import com.abhi.source.model.Usage;

public class Processor implements ItemProcessor<Usage, Usage> {
	@Override
	public Usage process(Usage usage) {

		Double billAmount = usage.getDataUsage() * .001 + usage.getMinutes() * .01;
		return new Usage(usage.getId(), usage.getFirstName(), usage.getLastName(),
				usage.getDataUsage(), usage.getMinutes());
	}

}
