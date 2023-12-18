#pragma once

#include <memory>
#include <vector>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <numeric>
#include <functional>
#include <cmath>
#include <mutex>

template <typename storageType, typename valueType>
class Hist
{
public:
	const int size;
	valueType from = 0;
	valueType to = 1;
   std::mutex lock;
	
	std::vector<storageType> histData;
	Hist(storageType size = 1, valueType from = 0, valueType to = 1)  : size(size), from(from), to(to) {
      std::unique_lock<std::mutex> ul(lock);
      histData.resize(size);
   }

	Hist(const Hist&) = delete;
	Hist& operator=(Hist&) = delete;
	
	int increaseSlot(valueType value) {
		if (value < from)
			value = from;
		if (value >= to)
			value = to-1;
		const valueType normalizedValue = (value - from) /(double) (to - from) * size;
		histData.at(normalizedValue)++;
		return normalizedValue;
	}
	
	void print() {
		//std::cout << "[";
		//std::cout << "(size:" << size << ",from:" << from << ",to:" << to << "),";
		std::copy(histData.begin(),
				  histData.end(),
				  std::ostream_iterator<storageType>(std::cout,",")
				  );
		//std::cout << "]";
	}
	
	valueType getPercentile(float iThPercentile) {
      std::unique_lock<std::mutex> ul(lock);
		long sum = std::accumulate(histData.begin(), histData.end(), 0, std::plus<int>());
		long sumUntilPercentile = 0;
		const long percentile = sum * (iThPercentile / 100.0);
		valueType i = 0;
		for (; sumUntilPercentile < percentile; i++) {
			sumUntilPercentile += histData[i];
		}
		return from + (i /(float) size * (to - from));
	}

	void writePercentiles(std::ostream& out) {
		out << getPercentile(50) << ","
			<< getPercentile(90) << ","
			<< getPercentile(95) << ","
			<< getPercentile(99) << ","
			<< getPercentile(99.5) << ","
			<< getPercentile(99.9) << ","
			<< getPercentile(99.99); 
	} 

	Hist& operator+=(const Hist<storageType, valueType> &rhs) {
		if (size != rhs.size || from != rhs.from || to != rhs.to ) {
			throw std::logic_error("Cannot add two different hists.");
		}
		for (int i = 0; i < size; i++) {
			this->histData[i] += rhs.histData[i];
		}
		return *this;
	}
	
	Hist& operator+(const Hist<storageType, valueType> &rhs) {
		auto result = *this;
		return result += rhs;
	}
	
	void resetData() {
		std::fill(histData.begin(), histData.end(), 0);
	}

private:
};
