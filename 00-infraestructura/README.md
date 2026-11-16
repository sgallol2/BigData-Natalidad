# Infraestructura en AWS

Esta guía crea, únicamente con AWS CLI, la infraestructura necesaria para
reproducir el proyecto:

- un bucket S3 con las zonas `raw`, `trusted`, `refined`, resultados de
  Athena y scripts;
- un security group;
- una instancia EC2 Ubuntu como fuente de archivos;
- una base de datos RDS MariaDB para las tablas de apoyo; y
- un clúster EMR temporal para Hive, Spark y PySpark.

Los comandos están escritos para **Windows CMD** y región `us-east-1`, que es
la región usada por AWS Academy en el proyecto. No se deben guardar
contraseñas, llaves ni endpoints reales en el repositorio.

## Conceptos para la sustentación

### ¿Qué papel cumple cada servicio?

- **Amazon S3:** almacenamiento central del data lake. Es durable, desacoplado
  del cómputo y organiza los datos por zonas (`raw`, `trusted` y `refined`).
- **EC2:** servidor Ubuntu que aloja temporalmente los CSV del DANE y ejecuta
  los scripts de ingesta.
- **RDS MariaDB:** fuente operacional estructurada para las dimensiones y
  catálogos de apoyo.
- **AWS Glue:** ejecuta jobs administrados de Spark para transformar `raw` a
  `trusted`.
- **EMR:** clúster temporal para Hive, Spark, PySpark y análisis distribuido.
- **Athena:** consulta archivos en S3 con SQL sin moverlos a una base de datos.

### Red y seguridad

La VPC y sus subnets proporcionan aislamiento de red. El security group
permite únicamente las conexiones necesarias: SSH restringido para administrar
la EC2 y MariaDB desde los recursos autorizados del proyecto. RDS no necesita
ser público porque la EC2 accede a él dentro de la red.

Los roles `LabRole` y `LabInstanceProfile` entregan permisos temporales a
Glue, EMR y EC2. Las credenciales de AWS no se escriben en los scripts; la
AWS CLI y `boto3` usan la cadena de credenciales del entorno.

### ¿Por qué S3 y no guardar todo en RDS?

RDS es apropiado para datos estructurados y transaccionales de apoyo, pero no
para conservar millones de filas de microdatos como archivos históricos.
S3 permite almacenar formatos originales, Parquet procesado y resultados,
mientras que cada motor de análisis accede al mismo almacenamiento.

### Costos y ciclo de vida

La EC2, RDS y especialmente EMR consumen recursos mientras están activos.
EMR se crea solo para análisis y debe terminarse al finalizar la sesión.
S3 conserva los datos importantes y evita depender del almacenamiento
temporal de HDFS.

## 1. Prerrequisitos

1. Iniciar una sesión de AWS Academy Learner Lab.
2. Instalar AWS CLI v2.
3. Configurar las credenciales temporales de la sesión:

   ```bat
   aws configure
   ```

   Usar la región `us-east-1`. Las credenciales deben ser las entregadas por
   Learner Lab y se deben actualizar cada vez que la sesión cambie.
4. Tener disponible la llave EC2 `bigdata-p2-key.pem` o crear una nueva desde
   EC2. La llave privada no se puede recuperar después de crearla.
5. Ejecutar:

   ```bat
   aws sts get-caller-identity
   ```

   El `Account` mostrado es el identificador de la cuenta que se usará en los
   siguientes comandos.

## 2. Variables de la sesión

Abrir una consola nueva y definir las variables. Cambiar solamente los valores
marcados como propios de la sesión:

```bat
set AWS_REGION=us-east-1
set AWS_DEFAULT_REGION=us-east-1
set PROJECT=bigdata-p2
set ACCOUNT_ID=REEMPLAZAR_CON_EL_ACCOUNT_DE_STS
set BUCKET=%PROJECT%-%ACCOUNT_ID%
set KEY_NAME=bigdata-p2-key
set KEY_FILE=bigdata-p2-key.pem
set EC2_TYPE=t3.small
set DB_ID=%PROJECT%-db
set DB_USER=admin
set DB_PASS=REEMPLAZAR_CON_UNA_CLAVE_SEGURA
```

También se pueden cargar las variables comunes del proyecto:

```bat
call ..\vars.cmd
```

No se debe versionar `vars.cmd` si contiene credenciales, contraseñas,
endpoints o identificadores de una sesión personal.

## 3. Validar la cuenta y elegir la red

AWS Academy normalmente proporciona una VPC por defecto. Obtener su
identificador y el de una subnet pública:

```bat
for /f "delims=" %i in ('aws ec2 describe-vpcs --filters Name=is-default,Values=true --query "Vpcs[0].VpcId" --output text') do set VPC_ID=%i
for /f "delims=" %i in ('aws ec2 describe-subnets --filters Name=vpc-id,Values=%VPC_ID% Name=default-for-az,Values=true --query "Subnets[0].SubnetId" --output text') do set SUBNET_ID=%i
for /f "delims=" %i in ('aws ec2 describe-subnets --filters Name=vpc-id,Values=%VPC_ID% Name=default-for-az,Values=true --query "Subnets[1].SubnetId" --output text') do set SUBNET_ID_2=%i
echo VPC=%VPC_ID% SUBNET=%SUBNET_ID% SUBNET_2=%SUBNET_ID_2%
```

Verificar que ambos valores no sean `None` y que las subnets pertenezcan a
zonas de disponibilidad diferentes:

```bat
aws ec2 describe-subnets --subnet-ids %SUBNET_ID% %SUBNET_ID_2% --query "Subnets[].{Id:SubnetId,AZ:AvailabilityZone,PublicIpOnLaunch:MapPublicIpOnLaunch}" --output table
```

Si no existe una VPC por defecto, seleccionar una VPC y dos subnets existentes
de la cuenta en lugar de crear una red adicional.

## 4. Crear el bucket y las zonas del data lake

En `us-east-1` el bucket se crea sin `--create-bucket-configuration`:

```bat
aws s3api create-bucket --bucket %BUCKET% --region %AWS_REGION%
aws s3api put-public-access-block --bucket %BUCKET% --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Crear los prefijos usados por las etapas del pipeline:

```bat
for %%P in (raw raw/archivos/nacimientos raw/rds raw/url trusted refined athena-results scripts) do aws s3api put-object --bucket %BUCKET% --key %%P/
aws s3 ls s3://%BUCKET%/
```

El bucket debe permanecer privado. Los jobs de Glue, EMR y Lambda deben usar
permisos IAM para acceder a él; no se debe publicar el bucket mediante una
bucket policy.

## 5. Crear el security group

El mismo security group permite que EC2, RDS y EMR se comuniquen dentro de la
VPC. El acceso SSH debe limitarse a la IP pública del administrador.

```bat
for /f "delims=" %i in ('aws ec2 create-security-group --group-name %PROJECT%-sg --description "Security group for BigData Natalidad" --vpc-id %VPC_ID% --query GroupId --output text') do set SG_ID=%i
for /f "delims=" %i in ('curl -s https://checkip.amazonaws.com') do set MY_IP=%i
aws ec2 authorize-security-group-ingress --group-id %SG_ID% --ip-permissions IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=%MY_IP%/32,Description=SSH}]
aws ec2 authorize-security-group-ingress --group-id %SG_ID% --ip-permissions IpProtocol=tcp,FromPort=3306,ToPort=3306,UserIdGroupPairs=[{GroupId=%SG_ID%,Description=MariaDB-from-project-resources}]
```

Si `curl` devuelve una dirección con espacios o no está disponible, sustituir
`%MY_IP%` manualmente por la IP pública actual seguida de `/32`. No abrir SSH
con `0.0.0.0/0`. Para confirmar el grupo:

```bat
aws ec2 describe-security-groups --group-ids %SG_ID% --query "SecurityGroups[0].{Id:GroupId,Name:GroupName,Rules:IpPermissions}" --output json
```

## 6. Crear la instancia EC2

Obtener la AMI oficial más reciente de Ubuntu Server 22.04 LTS:

```bat
for /f "delims=" %i in ('aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id --query Parameter.Value --output text') do set AMI_ID=%i
echo AMI=%AMI_ID%
```

`LabInstanceProfile` es el perfil proporcionado por AWS Academy. Crear la
instancia con IP pública para cargar los archivos DANE:

```bat
for /f "delims=" %i in ('aws ec2 run-instances --image-id %AMI_ID% --instance-type %EC2_TYPE% --key-name %KEY_NAME% --security-group-ids %SG_ID% --subnet-id %SUBNET_ID% --iam-instance-profile Name=LabInstanceProfile --associate-public-ip-address --block-device-mappings DeviceName=/dev/sda1,Ebs={VolumeSize=50,VolumeType=gp3,DeleteOnTermination=true} --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=%PROJECT%-files}]" --query "Instances[0].InstanceId" --output text') do set EC2_ID=%i
aws ec2 wait instance-running --instance-ids %EC2_ID%
for /f "delims=" %i in ('aws ec2 describe-instances --instance-ids %EC2_ID% --query "Reservations[0].Instances[0].PublicIpAddress" --output text') do set EC2_IP=%i
echo EC2=%EC2_ID% IP=%EC2_IP%
```

Probar el acceso cuando la instancia esté lista:

```bat
ssh -i %KEY_FILE% ubuntu@%EC2_IP%
```

En la EC2 se deben instalar las dependencias y cargar los CSV siguiendo
[`02-fuentes`](../02-fuentes). La IP pública puede cambiar si la instancia se
detiene; consultar siempre `describe-instances` antes de conectarse.

## 7. Crear RDS MariaDB

RDS necesita un DB subnet group con dos subnets en zonas diferentes:

```bat
aws rds create-db-subnet-group --db-subnet-group-name %PROJECT%-subnets --db-subnet-group-description "Subnets for BigData Natalidad" --subnet-ids %SUBNET_ID% %SUBNET_ID_2% --tags Key=Project,Value=%PROJECT%
```

Crear la instancia privada. El security group permite la conexión desde EC2
y desde otros recursos del proyecto:

```bat
aws rds create-db-instance --db-instance-identifier %DB_ID% --db-instance-class db.t3.micro --engine mariadb --engine-version 10.6 --allocated-storage 20 --storage-type gp3 --master-username %DB_USER% --master-user-password %DB_PASS% --db-subnet-group-name %PROJECT%-subnets --vpc-security-group-ids %SG_ID% --no-publicly-accessible --backup-retention-period 0 --no-multi-az --tags Key=Project,Value=%PROJECT%
aws rds wait db-instance-available --db-instance-identifier %DB_ID%
for /f "delims=" %i in ('aws rds describe-db-instances --db-instance-identifier %DB_ID% --query "DBInstances[0].Endpoint.Address" --output text') do set DB_HOST=%i
echo DB_HOST=%DB_HOST%
```

Guardar `DB_HOST` solamente en la configuración local (`vars.cmd` ignorado por
Git). El esquema y las tablas de apoyo se cargan desde
[`02-fuentes`](../02-fuentes).

## 8. Crear el clúster EMR temporal

El clúster se crea solo cuando se necesite ejecutar Hive, Spark o PySpark. En
AWS Academy se usan los roles administrados `LabRole` y
`LabInstanceProfile`; no se deben crear roles IAM nuevos.

```bat
aws emr create-cluster --name %PROJECT%-emr --release-label emr-6.15.0 --applications Name=Hadoop Name=Hive Name=Spark Name=JupyterEnterpriseGateway --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge --ec2-attributes InstanceProfile=LabInstanceProfile,SubnetId=%SUBNET_ID% --service-role LabRole --log-uri s3://%BUCKET%/emr-logs/ --use-default-roles --visible-to-all-users --ebs-root-volume-size 30 --tags Project=%PROJECT%
```

Si la cuenta no acepta `--use-default-roles` porque AWS Academy ya expone
`LabRole`, repetir el comando sin esa opción. Consultar el estado:

```bat
aws emr list-clusters --active
aws emr describe-cluster --cluster-id ID_DEL_CLUSTER --query "Cluster.{State:Status.State,Master:MasterPublicDnsName,Applications:Applications[].Name}" --output table
```

Cuando el clúster esté `WAITING`, seguir
[`05-catalogacion`](../05-catalogacion) y [`07-pyspark`](../07-pyspark). Todo
resultado importante debe quedar en S3, no solamente en HDFS.

## 9. Verificación final

```bat
aws s3 ls s3://%BUCKET%/
aws ec2 describe-instances --instance-ids %EC2_ID% --query "Reservations[0].Instances[0].{State:State.Name,PublicIp:PublicIpAddress}" --output table
aws rds describe-db-instances --db-instance-identifier %DB_ID% --query "DBInstances[0].{Status:DBInstanceStatus,Endpoint:Endpoint.Address}" --output table
aws emr list-clusters --active --query "Clusters[].{Id:Id,Name:Name,State:Status.State}" --output table
```

El pipeline completo se ejecuta en el orden indicado en el
[README principal](../README.md). La infraestructura no reemplaza los pasos de
ingesta, Glue, catalogación, consultas ni visualización.

## 10. Eliminación para evitar costos

Antes de terminar la sesión, eliminar el clúster EMR. Esta operación es
irreversible:

```bat
aws emr terminate-clusters --cluster-ids ID_DEL_CLUSTER
```

Cuando ya no se necesite la base de datos:

```bat
aws rds delete-db-instance --db-instance-identifier %DB_ID% --skip-final-snapshot
```

Eliminar la EC2 después de confirmar que los datos importantes están en S3:

```bat
aws ec2 terminate-instances --instance-ids %EC2_ID%
aws ec2 wait instance-terminated --instance-ids %EC2_ID%
```

Eliminar el DB subnet group y el security group cuando no tengan recursos
asociados:

```bat
aws rds delete-db-subnet-group --db-subnet-group-name %PROJECT%-subnets
aws ec2 delete-security-group --group-id %SG_ID%
```

El bucket debe conservarse mientras se necesite reproducir el proyecto. Para
eliminarlo por completo primero hay que borrar todos sus objetos:

```bat
aws s3 rb s3://%BUCKET% --force
```

`--force` elimina los objetos del bucket; usarlo únicamente después de
confirmar que existe una copia de los datos necesarios.
