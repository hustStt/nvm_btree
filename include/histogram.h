/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-20 17:01:43
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#ifndef _HBKV_HISTOGRAM_H_
#define _HBKV_HISTOGRAM_H_

#include <string>

namespace hbkv {

class Histogram {
public:
    Histogram() { Clear(); }
    ~Histogram() {}

    void Clear();
    void Add(double value);
    void Merge(const Histogram& other);

    std::string ToString() const;

private:
    enum { kNumBuckets = 154 };

    double Median() const;
    double Percentile(double p) const;
    double Average() const;
    double StandardDeviation() const;

    static const double kBucketLimit[kNumBuckets];

    double min_;
    double max_;
    double num_;
    double sum_;
    double sum_squares_;

    double buckets_[kNumBuckets];
};

}  //namespace name



#endif