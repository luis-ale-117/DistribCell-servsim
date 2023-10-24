# DistribCell-servsim
Repo with the server side simulation devolpment of the Cellular Automaton project for TT

## Queries to create database

```sql
CREATE TABLE Usuarios ( 
    id int auto_increment primary key, 
    nombre varchar(255) not null, 
    apellido varchar(255) not null, 
    correo varchar(255) not null unique, 
    contrasena varchar(255) not null 
);

CREATE TABLE Proyectos ( 
    id int auto_increment primary key, 
    usuario_id int, 
    nombre varchar(255) not null, 
    descripcion varchar(1024), 
    estado varchar(16) not null, 
    ultima_actualizacion bigint not null, 
    proceso_id bigint, 
    foreign key (usuario_id) references Usuarios(id)
);
CREATE TABLE Automatas ( 
    generacion int, 
    proyecto_id int, 
    contenido longblob, 
    primary key (generacion,proyecto_id) 
);
```