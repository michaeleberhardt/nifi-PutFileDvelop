This is a custom processor for apache nifi.
It is able to connect to d.velop documents cloud & onPremise to upload document-files and to set their properties.

Installation:
  1. Copy nifi-putfiledvelop-nar-0.0.1.nar file to the lib subdirectory of your nifi-installation
  2. Restart nifi.

Usage:
  For accessing d.velop documents and uploading files, three properties are mandatory:
  1. API-Key: Api-Key with sufficient rights to upload files to desired category
  2. Base-URI: the base-uri of your cloud-tenant or your onPremise instance of d.velop documents
  3. SourceCategory: the ID of the source category, the files should be member of

  Additional document properties can be added as custom properties with the prefix "dv_".
  For example, if your property-id in d.velop documents is 594453f0-ce71-4b2b-b369-5f480e8fbd22, in nifi that property has to be named dv_594453f0-ce71-4b2b-b369-5f480e8fbd22.
  Nifi expression language can be used for every property.
