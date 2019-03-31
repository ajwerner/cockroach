// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package aws

import (
	"encoding/json"
	"io/ioutil"
	"sort"
)

// awsConfigValue is a pflag.Value which configures how roachprod sets up
// instances with regards to VPC.
//
// The struct is populated by a call to Set which expects a file path for
// a serialized json structure that looks like the following:
//
//  {
//    "regions": {
//      "sensitive": false,
//      "type": "list",
//      "value": [
//          {
//              "ami_id": "ami-48630c2e",
//              "region": "ap-northeast-1",
//              "security_group": "sg-0006e480d77a10104",
//              "subnets": {
//                  "ap-northeast-1a": "subnet-0d144db3c9e47edf5",
//                  "ap-northeast-1c": "subnet-02fcaaa6212fc3c1a",
//                  "ap-northeast-1d": "subnet-0e9006ef8b3bef61f"
//              }
//          }
//      ]
//  }
//
// Logic has been added in the UnmarshalJSON method of availabilityZone and
// awsConfigValue to properly initialize the data structures.
type awsConfigValue struct {
	path     string
	azByName map[string]*availabilityZone

	// Regions mirrors the json structure of the output from terraform.
	Regions struct {
		Value []awsRegion `json:"value"`
	} `json:"regions"`
}

type awsRegion struct {
	Region            string            `json:"region"`
	SecurityGroup     string            `json:"security_group"`
	AMI               string            `json:"ami_id"`
	AvailabilityZones availabilityZones `json:"subnets"`
}

type availabilityZones []availabilityZone

type availabilityZone struct {
	name     string
	subnetID string
	region   *awsRegion // set up in awsConfig.UnmarshalJSON
}

// Set implements pflag.Value.
func (c *awsConfigValue) Set(path string) (err error) {
	c.path = path
	var data []byte
	if path == "" {
		data, err = Asset("config.json")
	} else {
		data, err = ioutil.ReadFile(path)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, c)
}

// Type implements pflag.Value.
func (c *awsConfigValue) Type() string {
	return "awsConfig"
}

// String implements pflag.Value.
func (c *awsConfigValue) String() string {
	if c.path == "" {
		return "(default, see config.json)"
	}
	return c.path
}

// UnmarshalJSON implement json.Unmarshaler.
//
// The extra logic is used to sort the data by region and to hook up the
// azByName map to point from AZ name to availabilityZone and to set up the
// backpointers from availabilityZone to region.
func (c *awsConfigValue) UnmarshalJSON(data []byte) error {
	type raw awsConfigValue
	var v raw
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	sort.Slice(v.Regions.Value, func(i, j int) bool {
		return v.Regions.Value[i].Region < v.Regions.Value[j].Region
	})
	v.azByName = make(map[string]*availabilityZone)
	for i := range v.Regions.Value {
		r := &v.Regions.Value[i]
		for i := range r.AvailabilityZones {
			az := &r.AvailabilityZones[i]
			az.region = r
			v.azByName[az.name] = az
		}
	}
	*c = awsConfigValue(v)
	return nil
}

func (c *awsConfigValue) regions() (regions []string) {
	for _, r := range c.Regions.Value {
		regions = append(regions, r.Region)
	}
	return regions
}

func (c *awsConfigValue) region(region string) *awsRegion {
	i := sort.Search(len(c.Regions.Value), func(i int) bool {
		return c.Regions.Value[i].Region >= region
	})
	if i < len(c.Regions.Value) && c.Regions.Value[i].Region == region {
		return &c.Regions.Value[i]
	}
	return nil
}

func (s availabilityZones) MarshalJSON() ([]byte, error) {
	m := make(map[string]string, len(s))
	for _, az := range s {
		m[az.name] = az.subnetID
	}
	return json.Marshal(m)
}

func (s *availabilityZones) UnmarshalJSON(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*s = make(availabilityZones, 0, len(m))
	for az, sn := range m {
		*s = append(*s, availabilityZone{
			name:     az,
			subnetID: sn,
		})
	}
	sort.Slice(*s, func(i, j int) bool {
		return (*s)[i].name < (*s)[j].name
	})
	return nil
}
