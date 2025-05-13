from xml.etree import ElementTree

# Grab values from specified field in XML record.
def confirm_field_values(tree_object, xml_attribute, xml_text):
    updated_values = []
    # Find relevant field value in ElementTree object.
    xml_string = ".//datafield[@" + xml_attribute + "='" + xml_text + "']"
    data_fields = tree_object.findall(xml_string)
    for data_field in data_fields:
        field_value = []
        subfields = data_field.findall('subfield')
        for subfield in subfields:
            subfield_attrib = subfield.attrib
            subfield_code = subfield_attrib['code']
            updated_value = subfield.text
            updated_value = subfield_code+' '+updated_value
            field_value.append(updated_value)
        field_value = ' '.join(field_value)
        updated_values.append(field_value)
    updated_values ='|'.join(updated_values)
    return updated_values

# Find and replace values from specified field in XML record.
def update_field_values(tree_object, xml_attribute, xml_text, value_pair):
    # Find relevant field value in ElementTree object and replace
        xml_string = ".//datafield[@"+xml_attribute+"='"+xml_text+"']"
        data_fields = tree_object.findall(xml_string)
        for data_field in data_fields:
            subfield = data_field.find('subfield')
            text_866 = subfield.text
            new = value_pair.get(text_866)
            if new is not None:
                subfield.text = new
            elif ";" in text_866:
                log['semicolon'] = True

def add_field(marc_field, marc_values):
    field_tag = marc_field
    field_info = marc_values
    ind1_value = field_info['ind1']
    ind2_value = field_info['ind2']
    subfields = field_info['subfields']
    new_field = ElementTree.Element('datafield', {'tag': field_tag, 'ind1': ind1_value, 'ind2': ind2_value})
    for key, value in subfields.items():
        subfield_code= key
        subfield_text = value
        new_subfield = ElementTree.SubElement(new_field,'subfield', {'code': subfield_code})
        new_subfield.text = subfield_text
    return new_field


def get_error_message(xml_response):
    try:
        print(xml_response.content)
        error_tree = ElementTree.fromstring(xml_response.content)
        error_list = error_tree.find('{http://com/exlibris/urm/general/xmlbeans}errorList')
        error = error_list.find('{http://com/exlibris/urm/general/xmlbeans}error')
        error_message = error.find('{http://com/exlibris/urm/general/xmlbeans}errorMessage')
        error_message = error_message.text
        put_error = error_message
    except ElementTree.ParseError:
        put_error = 'PUT parse error'
    return put_error
