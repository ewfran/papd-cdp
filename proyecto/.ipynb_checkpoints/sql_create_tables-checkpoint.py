CREATE_DW = '''
create table if not exists dim_compania(
	cod_com varchar(3) primary key,
	nom_com varchar(200), 
	nom_pai varchar(50),
	cco_sap varchar(50)
);

create table if not exists dim_encargado(
	cod_enc varchar(3) primary key,
	nom_enc varchar(50),
	tip_enc varchar(50)
);


create table if not exists dim_proveedor(
	cod_pro varchar(6) primary key,
	nom_pro varchar(50)
);

create table if not exists dim_prestacion(
	cod_pre varchar(3) primary key,
	des_pre varchar(50)
);

create table if not exists dim_area(
	cti_pro varchar(2) primary key,
	nti_pro varchar(50),
	area varchar(50)
);


create table if not exists dim_pais(
	cod_pai varchar(3) primary key,
	nom_pai varchar(50)
);


create table if not exists dim_departamento(
	cod_dep varchar(2) primary key,
	nom_dep varchar(50),
	cab_dep varchar(50),
	nom_pai varchar(50)
);


create table if not exists dim_municipio(
	sk_mun int primary key,
	cod_dep varchar(2),
	cod_mun varchar(2),
	nom_mun varchar(50)
);


create table if not exists dim_fecha(
	id_fecha varchar(50) primary key,
	year int,
	month int,
	quarter int,
	day int,
    week int,
	dayofweek int,
	is_weekend int,
	fecha date
);

-- tabla de hechos
create table if not exists fact_expediente(
	sk_exp int primary key,
	num_pol varchar(20),
	cod_com varchar(3),
	num_exp varchar(10),
	cti_pro varchar(2),
	cod_pai varchar(3),
	cod_dep varchar(20),
	cod_mun varchar(20),
	cod_zon varchar(20),
	hor_ser time,
	marca varchar(25),
	ano varchar(4),
	placa varchar(12),
	id_fecha varchar(50),
	cod_pro varchar(6),
	cod_pre varchar(3),
	cos_rea decimal(10,2),
	cod_enc varchar(3),
	
			
	constraint fk_fact_area
		foreign key (cti_pro)
			references dim_area(cti_pro),
			
	constraint fk_fact_pais
		foreign key (cod_pai)
			references dim_pais(cod_pai),
		
	constraint fk_fact_prestacion
		foreign key (cod_pre)
			references dim_prestacion(cod_pre)
	

);
'''