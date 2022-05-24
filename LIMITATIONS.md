# Known Issues, Limitations

The issues and limitations identified during the development of the Cognite Data Fusion (CDF) and Azure Digital Twins (ADT) plug-in are summarized in the table below.

<table>
<thead>
    <tr>
        <th style="border-right: 1px solid; width: 50%; text-align: center">Limitation</th>
        <th style="text-align: center">Solution (temporary)</th>
    </tr>
</thead>
<tbody>
    <tr>
        <td style="border-right: 1px solid">
        In ADT the digital twin ID ($dtId) cannot contain whitespace and colon (“;”) characters.
        </td>
        <td>replaced by underscore (“_”) and star (“*”)</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In ADT map keys cannot contain these characters: $, ., &lt;space&gt;
        </td>
        <td>replaced by # ^ _</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In CDF different type of resources can have the same external ID (e.g., an asset and a timeseries).
        </td>
        <td style="color:red">NOT HANDLED</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In CDF the external ID can be changed (or we can even switch them between 2 resources of the same type). Also, the “externalId” property in ADT can be edited, even when “writable” is set to False.
        </td>
        <td><span style="color:red">NOT HANDLED</span> – because the external ID is the digital twin ID in ADT, which is unique and cannot be changed</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In CDF relationships can exist by their own (even if the source and target assets were deleted), in ADT not (relationships are deleted automatically when a twin is deleted).
        </td>
        <td style="color:red">NOT HANDLED</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In ADT relationship ID’s are not necessarily unique. They are unique only within the source digital twin.
        </td>
        <td style="color:red">NOT HANDLED</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        CDF relationships can have multiple labels.
        </td>
        <td>
        The “relatesTo” ADT relationship has a “labels” property which holds the multiple labels separated by comma (“,”). DO NOT USE space before/after the comma.</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        A digital twin in ADT can be detached from a hierarchy, when the linking relationship is deleted.
        </td>
        <td><span style="color:red">NOT HANDLED</span>
        <ul>
            <li>It is the users’ responsibility</li>
            <li>PROBLEM: CDF->ADT synchronization will add it back.</li>
            <li>PROBLEM: this can happen to an entire subtree too</li>
        </ul>
        </td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        The length of a SQL query in ADT can have a maximum of 8000 characters, and the length of the list for the IN operation can have a maximum of 100 elements.
        </td>
        <td>
        <ul>
            <li>The list for the IN operation is divided into batches of 100, and the results of the multiple SQL queries are collected in a common list. </li>
            <li>The 8000 characters length SQL problem is 
            <span style="color:red">NOT HANDLED</span></li>
        </ul>        
        </td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In CDF <b>a root node cannot be linked to a hierarchy</b> (it’s parent cannot be set). In CDF an asset must stay in the same hierarchy. But in ADT we first create a node, then set the parent with a new relationship.
        </td>
        <td>
        A new asset from ADT is linked at first to the root in CDF.
        <p>PROBLEM: a single hierarchy is handled by the ADT->CDF sync.</p>
        <p style="background: black;">ALTERNATE SOLUTION (not implemented currently): delete the asset and recreate when parent is set, but in this case entire subtrees might need to be recreated. </p>      
        </td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        In ADT an asset can have multiple parents, even though the “maxMultiplicity” is set to 1. Similarly, a timeseries can be linked to multiple assets.
        </td>
        <td>An error is logged. And in CDF the parent is changed when the actual parent relationship is deleted in ADT.</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">In CDF the "is_string" property of timeseries cannot be updated (timeseries are numeric by default).</td>
        <td>When a value is inserted into a timeseries in ADT (together with the timestamp):
        <ul>
            <li>if it is the first value and a string, the timeseries in CDF is deleted and recreated as string-valued; then the new value is inserted
            </li>
            <li>if it is not the first value, it is verified that numeric values are inserted only into numeric, and string values only into string-valued timeseries, respectively; otherwise an error message is shown.
            </li>
        </ul>
        </td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        Unexpected errors can happen, when ADT tries do delete a resource, but CDF has modified it in the same time. Same goes for inverse sync.
        </td>
        <td style="color:red">NOT HANDLED</td>
    </tr>
    <tr>
        <td style="border-right: 1px solid">
        If the events from the event-hub are not in order, then the ADT->CDF can cause unexpected errors.
        </td>
        <td style="color:red">NOT HANDLED</td>
    </tr>
</tbody>
</table>