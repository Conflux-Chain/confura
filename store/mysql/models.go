package mysql

var allModels = []interface{}{
	&transaction{},
	&block{},
	&log{},
	&epochStats{},
	&conf{},
	&User{},
	&Contract{},
}
